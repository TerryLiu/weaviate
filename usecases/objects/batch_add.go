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

package objects

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/versioned"

	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/objects/validation"
)

// errEmptyObjects 当批量操作的对象参数为空时返回的错误
// 用于验证批量添加请求的有效性
var errEmptyObjects = NewErrInvalidUserInput("invalid param 'objects': cannot be empty, need at least one object for batching")

// AddObjects 批量添加类实例到连接的数据库
// 参数:
//   ctx: 上下文，用于超时控制和取消操作
//   principal: 用户凭证信息，包含身份认证数据
//   objects: 待添加的对象数组
//   fields: 返回字段过滤器
//   repl: 复制属性配置
// 返回值:
//   BatchObjects: 批量操作结果数组
//   error: 错误信息，如果操作失败则返回具体错误
func (b *BatchManager) AddObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, fields []*string, repl *additional.ReplicationProperties,
) (BatchObjects, error) {
	// 将类缓存添加到上下文中以提高性能
	ctx = classcache.ContextWithClassCache(ctx)

	// 构建类名到分片的映射关系，用于后续权限验证
	classesShards := make(map[string][]string)
	for _, obj := range objects {
		// 将类名转换为大写格式（Weaviate标准）
		obj.Class = schema.UppercaseClassName(obj.Class)
		// 解析可能存在的别名，获取真实的类名
		cls, _ := b.resolveAlias(obj.Class)
		obj.Class = cls
		// 按类名分组存储租户信息
		classesShards[obj.Class] = append(classesShards[obj.Class], obj.Tenant)
	}
	// 存储已知的类定义信息，避免重复查询
	knownClasses := map[string]versioned.Class{}

	// 整个请求会在任一集合缺少权限时失败
	// 这样可以防止部分成功导致的数据不一致
	for className, shards := range classesShards {
		// 不泄露插入数据用户本来就不具备的信息
		// 直接从缓存获取类定义，无需额外权限检查
		vClass, err := b.schemaManager.GetCachedClassNoAuth(ctx, className)
		if err != nil {
			return nil, err
		}
		knownClasses[className] = vClass[className]

		// 验证UPDATE权限 - 确保用户可以更新指定分片的数据
		if err := b.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.ShardsData(className, shards...)...); err != nil {
			return nil, err
		}

		// 验证CREATE权限 - 确保用户可以在指定分片创建新对象
		if err := b.authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.ShardsData(className, shards...)...); err != nil {
			return nil, err
		}
	}

	// 调用内部方法执行实际的批量添加操作
	return b.addObjects(ctx, principal, objects, repl, knownClasses)
}

// AddObjectsGRPCAfterAuth 绕过REST端点的认证检查，因为gRPC有自己的认证机制
// 此方法专为gRPC接口设计，避免重复的认证开销
// 参数fetchedClasses是从外部传入的已获取的类定义信息
func (b *BatchManager) AddObjectsGRPCAfterAuth(ctx context.Context, principal *models.Principal,
	objects []*models.Object, repl *additional.ReplicationProperties, fetchedClasses map[string]versioned.Class,
) (BatchObjects, error) {
	// 直接调用内部批量添加方法
	return b.addObjects(ctx, principal, objects, repl, fetchedClasses)
}

// addObjects 执行批量对象添加的核心逻辑
// 这是批量添加操作的主要实现方法
func (b *BatchManager) addObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, repl *additional.ReplicationProperties, fetchedClasses map[string]versioned.Class,
) (BatchObjects, error) {
	// 再次确保上下文中包含类缓存（双重保险）
	ctx = classcache.ContextWithClassCache(ctx)

	// 开始记录批量操作指标
	before := time.Now()
	// 增加批量操作计数器
	b.metrics.BatchInc()
	// 在函数结束时记录总耗时并减少计数器
	defer b.metrics.BatchOp("total_uc_level", before.UnixNano())
	defer b.metrics.BatchDec()

	// 记录预处理阶段开始时间
	beforePreProcessing := time.Now()
	// 检查输入对象列表是否为空
	if len(objects) == 0 {
		return nil, errEmptyObjects
	}

	// 存储最大的schema版本号，用于后续的版本同步
	var maxSchemaVersion uint64
	// 验证对象并获取向量表示，同时返回使用的最大schema版本
	batchObjects, maxSchemaVersion := b.validateAndGetVector(ctx, principal, objects, repl, fetchedClasses)
	// 自动创建缺失的租户，并返回新的schema版本和创建的租户数量
	schemaVersion, tenantCount, err := b.autoSchemaManager.autoTenants(ctx, principal, objects, fetchedClasses)
	if err != nil {
		return nil, fmt.Errorf("auto create tenants: %w", err)
	}
	// 更新最大schema版本号
	if schemaVersion > maxSchemaVersion {
		maxSchemaVersion = schemaVersion
	}

	// 记录性能指标
	b.metrics.BatchTenants(tenantCount)           // 记录处理的租户数量
	b.metrics.BatchObjects(len(objects))          // 记录处理的对象数量
	b.metrics.BatchOp("total_preprocessing", beforePreProcessing.UnixNano()) // 记录预处理耗时

	// 声明结果变量
	var res BatchObjects

	// 记录持久化阶段开始时间
	beforePersistence := time.Now()
	// 在函数结束时记录持久化阶段总耗时
	defer b.metrics.BatchOp("total_persistence_level", beforePersistence.UnixNano())

	// 确保本地schema已经更新到我们用于验证的版本
	// 这是为了保证数据一致性和避免版本冲突
	if err := b.schemaManager.WaitForUpdate(ctx, maxSchemaVersion); err != nil {
		return nil, fmt.Errorf("error waiting for local schema to catch up to version %d: %w", maxSchemaVersion, err)
	}
	
	// 调用存储层执行批量对象持久化操作
	// 传入验证后的对象、复制配置和schema版本
	if res, err = b.vectorRepo.BatchPutObjects(ctx, batchObjects, repl, maxSchemaVersion); err != nil {
		return nil, NewErrInternal("batch objects: %#v", err)
	}

	return res, nil
}

// validateAndGetVector 验证批量对象并获取向量表示
// 这是批量添加流程中的核心验证步骤
// 参数:
//   ctx: 上下文
//   principal: 用户凭证
//   objects: 待验证的对象数组
//   repl: 复制属性
//   fetchedClasses: 已获取的类定义映射
// 返回值:
//   BatchObjects: 验证后的批量对象结果
//   uint64: 使用的最大schema版本号
func (b *BatchManager) validateAndGetVector(ctx context.Context, principal *models.Principal,
	objects []*models.Object, repl *additional.ReplicationProperties, fetchedClasses map[string]versioned.Class,
) (BatchObjects, uint64) {
	// 初始化各种必要的变量和数据结构
	var (
		now          = time.Now().UnixNano() / int64(time.Millisecond) // 获取当前时间戳（毫秒）
		batchObjects = make(BatchObjects, len(objects))                // 创建结果数组

		objectsPerClass       = make(map[string][]*models.Object)      // 按类名分组的对象
		originalIndexPerClass = make(map[string][]int)                 // 保存原始索引映射
		validator             = validation.New(b.vectorRepo.Exists, b.config, repl) // 创建验证器实例
	)

	// 验证每个对象并按类名分组（相同类名使用相同的向量化器）
	var maxSchemaVersion uint64
	for i, obj := range objects {
		// 保存原始索引，用于后续结果映射
		batchObjects[i].OriginalIndex = i

		// 检查对象类名是否为空
		if obj.Class == "" {
			batchObjects[i].Err = errors.New("object has an empty class")
			continue
		}

		// 自动推断和创建schema（如果需要）
		schemaVersion, err := b.autoSchemaManager.autoSchema(ctx, principal, true, fetchedClasses, obj)
		if err != nil {
			batchObjects[i].Err = err
		}
		// 更新最大schema版本号
		if schemaVersion > maxSchemaVersion {
			maxSchemaVersion = schemaVersion
		}

		// 处理对象ID：自动生成或验证现有ID
		if obj.ID == "" {
			// 为新对象生成UUID
			uid, err := generateUUID()
			obj.ID = uid
			batchObjects[i].Err = err
		} else {
			// 验证现有ID的格式是否正确
			if _, err := uuid.Parse(obj.ID.String()); err != nil {
				batchObjects[i].Err = err
			}
		}
		// 初始化对象属性（如果为nil则创建空map）
		if obj.Properties == nil {
			obj.Properties = map[string]interface{}{}
		}
		// 设置创建和更新时间戳
		obj.CreationTimeUnix = now
		obj.LastUpdateTimeUnix = now
		// 将处理后的对象存储到结果中
		batchObjects[i].Object = obj
		batchObjects[i].UUID = obj.ID
		// 如果前面的步骤中有错误，则跳过后续验证
		if batchObjects[i].Err != nil {
			continue
		}

		// 验证类定义是否存在
		if len(fetchedClasses) == 0 || fetchedClasses[obj.Class].Class == nil {
			batchObjects[i].Err = fmt.Errorf("class '%v' not present in schema", obj.Class)
			continue
		}
		class := fetchedClasses[obj.Class].Class

		// 使用验证器对对象进行结构验证
		if err := validator.Object(ctx, class, obj, nil); err != nil {
			batchObjects[i].Err = err
			continue
		}

		// 按类名分组存储对象，以便批量向量化
		if objectsPerClass[obj.Class] == nil {
			objectsPerClass[obj.Class] = make([]*models.Object, 0)
			originalIndexPerClass[obj.Class] = make([]int, 0)
		}
		objectsPerClass[obj.Class] = append(objectsPerClass[obj.Class], obj)
		originalIndexPerClass[obj.Class] = append(originalIndexPerClass[obj.Class], i)
	}

	// 对每个类别的对象进行批量向量化处理
	for className, objectsForClass := range objectsPerClass {
		class := fetchedClasses[className]
		// 调用模块提供者进行批量向量更新
		errorsPerObj, err := b.modulesProvider.BatchUpdateVector(ctx, class.Class, objectsForClass, b.findObject, b.logger)
		if err != nil {
			// 如果整个类别向量化失败，将错误应用到该类别所有对象
			for i := range objectsForClass {
				origIndex := originalIndexPerClass[className][i]
				batchObjects[origIndex].Err = err
			}
		}
		// 将单个对象的向量化错误映射回原始位置
		for i, err := range errorsPerObj {
			origIndex := originalIndexPerClass[className][i]
			batchObjects[origIndex].Err = err
		}
	}

	return batchObjects, maxSchemaVersion
}
