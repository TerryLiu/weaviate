//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/objects"
)

// batchQueue 批量队列结构体，用于存储待处理的对象及其原始索引
// objects: 存储实际的对象数据
// originalIndex: 记录每个对象在原始批次中的索引位置
type batchQueue struct {
	objects       []*storobj.Object
	originalIndex []int
}

// BatchPutObjects 批量插入对象到数据库
// ctx: 上下文对象，用于控制超时和取消
// objs: 待插入的对象批次
// repl: 复制属性配置
// schemaVersion: 模式版本号
// 返回值: 处理后的对象批次和可能的错误
func (db *DB) BatchPutObjects(ctx context.Context, objs objects.BatchObjects,
	repl *additional.ReplicationProperties, schemaVersion uint64,
) (objects.BatchObjects, error) {
	// 按类别分组存储对象和索引映射
	objectByClass := make(map[string]batchQueue)  // 按类名分组的对象队列
	indexByClass := make(map[string]*Index)       // 按类名映射的索引实例

	// 内存检查逻辑：仅当异步索引未启用时进行内存检查。
	// 如果启用了异步索引，内存分配决策会推迟到出队步骤执行，
	// 这样可以避免入队被阻塞，并防止因入队过快导致出队时意外OOM。
	if !db.AsyncIndexingEnabled {
		// 检查是否有足够内存处理当前批次
		if err := db.memMonitor.CheckAlloc(estimateBatchMemory(objs)); err != nil {
			db.logger.WithError(err).Errorf("memory pressure: cannot process batch")
			return nil, fmt.Errorf("cannot process batch: %w", err)
		}
	}

	// 遍历所有对象，将有效对象按类别分组
	for _, item := range objs {
		if item.Err != nil {
			// 跳过有验证错误或其他忽略原因的项目
			continue
		}
		// 获取当前类别的对象队列，并提取向量信息
		queue := objectByClass[item.Object.Class]
		vectors, multiVectors, err := dto.GetVectors(item.Object.Vectors)
		if err != nil {
			return nil, fmt.Errorf("cannot process batch: cannot get vectors: %w", err)
		}
		// 将对象转换为存储格式并添加到队列中
		queue.objects = append(queue.objects, storobj.FromObject(item.Object, item.Object.Vector, vectors, multiVectors))
		queue.originalIndex = append(queue.originalIndex, item.OriginalIndex)
		objectByClass[item.Object.Class] = queue
	}

	// 使用匿名函数包装，确保只在循环期间获取和安全释放indexLock
	func() {
		db.indexLock.RLock()
		defer db.indexLock.RUnlock()

		// 遍历每个类别，获取对应的索引实例
		for class, queue := range objectByClass {
			index, ok := db.indices[indexID(schema.ClassName(class))]
			if !ok {
				// 如果找不到索引，可能是该类别已被删除
				msg := fmt.Sprintf("could not find index for class %v. It might have been deleted in the meantime", class)
				db.logger.Warn(msg)
				// 为该队列中的所有对象设置错误信息
				for _, origIdx := range queue.originalIndex {
					if origIdx >= len(objs) {
						db.logger.Errorf(
							"batch add queue index out of bounds. len(objs) == %d, queue.originalIndex == %d",
							len(objs), origIdx)
						break
					}
					objs[origIdx].Err = errors.New(msg)
				}
				continue
			}
			// 获取索引的读锁并建立类别到索引的映射
			index.dropIndex.RLock()
			indexByClass[class] = index
		}
	}()

	// 安全释放剩余锁（防止panic情况下的锁泄露）
	defer func() {
		for _, index := range indexByClass {
			if index != nil {
				index.dropIndex.RUnlock()
			}
		}
	}()

	// 对每个类别执行批量插入操作
	for class, index := range indexByClass {
		queue := objectByClass[class]
		// 执行批量对象插入
		errs := index.putObjectBatch(ctx, queue.objects, repl, schemaVersion)
		// 更新指标统计
		index.metrics.BatchCount(len(queue.objects))
		index.metrics.BatchCountBytes(estimateStorBatchMemory(queue.objects))

		// 从映射中移除索引以跳过defer中的锁释放
		indexByClass[class] = nil
		index.dropIndex.RUnlock()
		// 将错误信息映射回原始对象数组
		for i, err := range errs {
			if err != nil {
				objs[queue.originalIndex[i]].Err = err
			}
		}
	}

	return objs, nil
}

// AddBatchReferences 批量添加对象引用关系
// ctx: 上下文对象
// references: 待添加的引用关系批次
// repl: 复制属性配置
// schemaVersion: 模式版本号
// 返回值: 处理后的引用关系批次和可能的错误
func (db *DB) AddBatchReferences(ctx context.Context, references objects.BatchReferences,
	repl *additional.ReplicationProperties, schemaVersion uint64,
) (objects.BatchReferences, error) {
	// 按类别分组存储引用关系和索引映射
	refByClass := make(map[schema.ClassName]objects.BatchReferences)  // 按类名分组的引用关系
	indexByClass := make(map[schema.ClassName]*Index)                // 按类名映射的索引实例

	// 遍历所有引用关系，按源对象类别进行分组
	for _, item := range references {
		if item.Err != nil {
			// 跳过有验证错误或其他忽略原因的项目
			continue
		}
		// 按源对象类别分组存储引用关系
		refByClass[item.From.Class] = append(refByClass[item.From.Class], item)
	}

	// 使用匿名函数包装，确保只在循环期间获取和安全释放indexLock
	func() {
		db.indexLock.RLock()
		defer db.indexLock.RUnlock()

		// 遍历每个类别，获取对应的索引实例
		for class, queue := range refByClass {
			index, ok := db.indices[indexID(class)]
			if !ok {
				// 如果找不到索引，为该队列中的所有引用设置错误
				for _, item := range queue {
					references[item.OriginalIndex].Err = fmt.Errorf("could not find index for class %v. It might have been deleted in the meantime", class)
				}
				continue
			}
			// 获取索引的读锁并建立映射
			index.dropIndex.RLock()
			indexByClass[class] = index
		}
	}()

	// 安全释放剩余锁（防止panic情况下的锁泄露）
	defer func() {
		for _, index := range indexByClass {
			if index != nil {
				index.dropIndex.RUnlock()
			}
		}
	}()

	// 对每个类别执行批量引用添加操作
	for class, index := range indexByClass {
		queue := refByClass[class]
		// 执行批量引用添加
		errs := index.AddReferencesBatch(ctx, queue, repl, schemaVersion)
		// 从映射中移除索引以跳过defer中的锁释放
		indexByClass[class] = nil
		index.dropIndex.RUnlock()
		// 将错误信息映射回原始引用数组
		for i, err := range errs {
			if err != nil {
				references[queue[i].OriginalIndex].Err = err
			}
		}
	}

	return references, nil
}

// BatchDeleteObjects 批量删除符合条件的对象
// ctx: 上下文对象
// params: 批量删除参数，包含过滤条件等
// deletionTime: 删除时间戳
// repl: 复制属性配置
// tenant: 租户标识
// schemaVersion: 模式版本号
// 返回值: 批量删除结果和可能的错误
func (db *DB) BatchDeleteObjects(ctx context.Context, params objects.BatchDeleteParams,
	deletionTime time.Time, repl *additional.ReplicationProperties, tenant string, schemaVersion uint64,
) (objects.BatchDeleteResult, error) {
	start := time.Now()
	// 获取指定类别的索引
	className := params.ClassName
	idx := db.GetIndex(className)
	if idx == nil {
		return objects.BatchDeleteResult{}, errors.Errorf("cannot find index for class %v", className)
	}

	// 查找所有分片中匹配过滤条件的文档ID
	shardDocIDs, err := idx.findUUIDs(ctx, params.Filters, tenant, repl, 0)
	if err != nil {
		return objects.BatchDeleteResult{}, errors.Wrapf(err, "cannot find objects")
	}
	// 准备从所有分片中待删除的文档ID列表
	toDelete := map[string][]strfmt.UUID{}  // 按分片名存储待删除的UUID
	limit := db.config.QueryMaximumResults   // 查询结果最大限制

	// 统计匹配的文档数量并按限制准备删除列表
	matches := int64(0)
	for shardName, docIDs := range shardDocIDs {
		docIDsLength := int64(len(docIDs))
		if matches <= limit {
			// 根据剩余配额决定是否全部删除或部分删除
			if matches+docIDsLength <= limit {
				toDelete[shardName] = docIDs
			} else {
				toDelete[shardName] = docIDs[:limit-matches]
			}
		}
		matches += docIDsLength
	}

	// 记录识别到的待删除对象信息
	db.logger.WithFields(logrus.Fields{
		"action":  "batch_delete_objects_post_find_ids",
		"params":  params,
		"tenant":  tenant,
		"matches": matches,
		"dry_run": params.DryRun,
		"took":    time.Since(start),
	}).Debugf("batch delete: identified %v objects to delete", matches)

	// 检查是否有足够内存执行批量删除操作
	if err := db.memMonitor.CheckAlloc(memwatch.EstimateObjectDeleteMemory() * matches); err != nil {
		db.logger.WithError(err).Errorf("memory pressure: cannot process batch delete object")
		return objects.BatchDeleteResult{}, fmt.Errorf("cannot process batch delete object: %w", err)
	}

	// 在指定分片中删除文档ID
	deletedObjects, err := idx.batchDeleteObjects(ctx, toDelete, deletionTime, params.DryRun, repl, schemaVersion, tenant)
	if err != nil {
		return objects.BatchDeleteResult{}, errors.Wrapf(err, "cannot delete objects")
	}

	// 构造批量删除结果
	result := objects.BatchDeleteResult{
		Matches:      matches,      // 匹配的对象总数
		Limit:        db.config.QueryMaximumResults,  // 查询限制
		DeletionTime: deletionTime,  // 删除时间
		DryRun:       params.DryRun,   // 是否为试运行
		Objects:      deletedObjects,  // 实际删除的对象列表
	}

	// 记录批量删除完成信息
	db.logger.WithFields(logrus.Fields{
		"action":  "batch_delete_objects_completed",
		"params":  params,
		"tenant":  tenant,
		"matches": matches,
		"took":    time.Since(start),
		"dry_run": params.DryRun,
	}).Debugf("batch delete completed in %s", time.Since(start))
	return result, nil
}

// estimateBatchMemory 估算批量对象的内存使用量
// objs: 批量对象
// 返回值: 预估的总内存使用量（字节）
func estimateBatchMemory(objs objects.BatchObjects) int64 {
	var sum int64
	for _, item := range objs {
		sum += memwatch.EstimateObjectMemory(item.Object)
	}

	return sum
}

// estimateStorBatchMemory 估算存储对象批次的内存使用量
// objs: 存储对象数组
// 返回值: 预估的总内存使用量（字节）
func estimateStorBatchMemory(objs []*storobj.Object) int64 {
	var sum int64
	for _, item := range objs {
		sum += memwatch.EstimateStorObjectMemory(item)
	}

	return sum
}
