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

package hnsw

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

// FilterStrategy 定义过滤策略类型
// 用于控制在搜索过程中如何处理允许列表(allowList)的过滤逻辑
type FilterStrategy int

const (
	// SWEEPING 扫描策略：在搜索过程中逐个检查每个候选节点是否在允许列表中
	SWEEPING FilterStrategy = iota
	// ACORN 自适应候选推荐网络策略：智能选择候选节点以提高过滤效率
	ACORN
	// RRE 基于入口点的重新评估策略：从入口点开始重新评估候选节点
	RRE
)

// searchTimeEF 计算搜索时的扩展因子(EF)
// EF决定了在搜索过程中考虑的候选节点数量，影响搜索精度和性能
// 参数 k: 请求返回的最近邻数量
// 返回值: 计算得到的EF值
func (h *hnsw) searchTimeEF(k int) int {
	// 原子加载EF值，避免并发读写冲突
	// 这样可以在不加锁的情况下处理用户配置的并发更新
	ef := int(atomic.LoadInt64(&h.ef))
	if ef < 1 {
		// 如果EF未设置或无效，则根据k值自动计算
		return h.autoEfFromK(k)
	}

	// 确保EF至少等于k，否则无法返回足够的结果
	if ef < k {
		ef = k
	}

	return ef
}

// autoEfFromK 根据k值自动计算扩展因子
// 使用公式: ef = k * factor，并确保在[min, max]范围内
// 参数 k: 请求返回的最近邻数量
// 返回值: 计算得到的EF值
func (h *hnsw) autoEfFromK(k int) int {
	// 原子加载配置参数
	factor := int(atomic.LoadInt64(&h.efFactor)) // EF因子
	min := int(atomic.LoadInt64(&h.efMin))       // EF最小值
	max := int(atomic.LoadInt64(&h.efMax))       // EF最大值

	// 计算基础EF值
	ef := k * factor
	
	// 边界检查：确保EF在合理范围内
	if ef > max {
		ef = max
	} else if ef < min {
		ef = min
	}
	
	// 最后确保EF至少等于k，防止结果被过早截断
	if k > ef {
		ef = k // 否则结果会被过早截断
	}

	return ef
}

// SearchByVector 根据向量进行K近邻搜索
// 参数:
//   ctx: 上下文，用于超时和取消控制
//   vector: 查询向量
//   k: 需要返回的最近邻数量
//   allowList: 允许列表，用于过滤结果(可为nil)
// 返回值:
//   []uint64: 匹配的对象ID列表
//   []float32: 对应的距离值列表
//   error: 错误信息
func (h *hnsw) SearchByVector(ctx context.Context, vector []float32,
	k int, allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	// 获取压缩操作读锁，防止在搜索过程中发生压缩操作
	h.compressActionLock.RLock()
	defer h.compressActionLock.RUnlock()

	// 对查询向量进行归一化处理
	vector = h.normalizeVec(vector)
	
	// 获取平面搜索阈值
	flatSearchCutoff := int(atomic.LoadInt64(&h.flatSearchCutoff))
	
	// 判断是否使用平面搜索：当有允许列表且允许列表较小且未禁用平面搜索时
	if allowList != nil && !h.forbidFlat && allowList.Len() < flatSearchCutoff {
		helpers.AnnotateSlowQueryLog(ctx, "hnsw_flat_search", true)
		// 使用平面搜索(暴力搜索)
		return h.flatSearch(ctx, vector, k, h.searchTimeEF(k), allowList)
	}
	
	helpers.AnnotateSlowQueryLog(ctx, "hnsw_flat_search", false)
	// 使用HNSW层次搜索
	return h.knnSearchByVector(ctx, vector, k, h.searchTimeEF(k), allowList)
}

// SearchByMultiVector 多向量搜索功能
// 支持对多个查询向量同时进行搜索并聚合结果
// 参数:
//   ctx: 上下文
//   vectors: 多个查询向量的切片
//   k: 需要返回的结果数量
//   allowList: 允许列表过滤器
// 返回值: 文档ID列表、距离列表和错误信息
func (h *hnsw) SearchByMultiVector(ctx context.Context, vectors [][]float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	// 检查多向量搜索是否启用
	if !h.multivector.Load() {
		return nil, nil, errors.New("multivector search is not enabled")
	}

	// 处理Muvera编码情况（多向量编码器）
	if h.muvera.Load() {
		// 仅当HNSW为空时发生，需要初始化muvera编码器
		if err := h.initMuveraEncoder(vectors); err != nil {
			return nil, nil, err
		}

		// 将多个查询向量编码为单个查询向量
		muvera_query := h.muveraEncoder.EncodeQuery(vectors)
		overfetch := 2 // 过度获取因子，获取更多候选结果
		
		// 使用编码后的查询向量进行搜索
		docIDs, _, err := h.SearchByVector(ctx, muvera_query, overfetch*k, allowList)
		if err != nil {
			return nil, nil, err
		}
		
		// 构建候选集
		candidateSet := make(map[uint64]struct{})
		for _, docID := range docIDs {
			candidateSet[docID] = struct{}{}
		}
		
		// 计算延迟交互得分
		return h.computeLateInteraction(vectors, k, candidateSet)
	}

	// 获取压缩操作读锁
	h.compressActionLock.RLock()
	defer h.compressActionLock.RUnlock()

	// 归一化所有查询向量
	vectors = h.normalizeVecs(vectors)
	flatSearchCutoff := int(atomic.LoadInt64(&h.flatSearchCutoff))
	
	// 判断是否使用平面多向量搜索
	if allowList != nil && !h.forbidFlat && allowList.Len() < flatSearchCutoff {
		helpers.AnnotateSlowQueryLog(ctx, "hnsw_flat_search", true)
		return h.flatMultiSearch(ctx, vectors, k, allowList)
	}
	
	helpers.AnnotateSlowQueryLog(ctx, "hnsw_flat_search", false)
	// 使用HNSW多向量搜索
	return h.knnSearchByMultiVector(ctx, vectors, k, allowList)
}

// SearchByVectorDistance 按距离阈值搜索
// 包装SearchByVector方法，递归调用直到搜索结果包含所有距离小于目标阈值的向量
//
// maxLimit参数限制返回的搜索结果数量上限。
// 当方法的结果最终要转换为对象时使用此参数，例如Get查询。
// 如果调用者只需要ID用于聚合等操作，可以传入-1来真正获取索引中的所有结果。
//
// 参数:
//   ctx: 上下文
//   vector: 查询向量
//   targetDistance: 目标距离阈值
//   maxLimit: 最大返回结果数(-1表示无限制)
//   allowList: 允许列表
// 返回值: 符合距离阈值的对象ID、距离值和错误信息
func (h *hnsw) SearchByVectorDistance(ctx context.Context, vector []float32,
	targetDistance float32, maxLimit int64,
	allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	return searchByVectorDistance(ctx, vector, targetDistance, maxLimit, allowList,
		h.SearchByVector, h.logger)
}

// SearchByMultiVectorDistance 多向量按距离阈值搜索
// 包装SearchByMultiVector方法，递归调用直到搜索结果包含所有距离小于目标阈值的向量
//
// maxLimit参数限制返回的搜索结果数量上限。
// 当方法的结果最终要转换为对象时使用此参数，例如Get查询。
// 如果调用者只需要ID用于聚合等操作，可以传入-1来真正获取索引中的所有结果。
//
// 参数:
//   ctx: 上下文
//   vector: 多个查询向量
//   targetDistance: 目标距离阈值
//   maxLimit: 最大返回结果数(-1表示无限制)
//   allowList: 允许列表
// 返回值: 符合距离阈值的对象ID、距离值和错误信息
func (h *hnsw) SearchByMultiVectorDistance(ctx context.Context, vector [][]float32,
	targetDistance float32, maxLimit int64,
	allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	return searchByVectorDistance(ctx, vector, targetDistance, maxLimit, allowList,
		h.SearchByMultiVector, h.logger)
}

// initMuveraEncoder 初始化Muvera编码器
// Muvera是一种多向量编码技术，用于将多个向量编码为单个向量进行高效搜索
// 参数 vectors: 多个向量的切片，用于确定编码器维度
// 返回值: 初始化错误信息
func (h *hnsw) initMuveraEncoder(vectors [][]float32) error {
	// 检查输入向量数组是否为空
	if len(vectors) == 0 {
		return fmt.Errorf("multi vector array is empty")
	}
	
	// 使用Once确保编码器只初始化一次
	h.trackMuveraOnce.Do(func() {
		// 初始化编码器，使用第一个向量的维度
		h.muveraEncoder.InitEncoder(len(vectors[0]))
		h.Lock()
		// 持久化Muvera配置到提交日志
		if err := h.muveraEncoder.PersistMuvera(h.commitLog); err != nil {
			h.Unlock()
			h.logger.WithField("action", "persist muvera").Error(err)
			return
		}
		h.Unlock()
	})
	return nil
}

// shouldRescore 判断是否需要重新评分
// 在使用量化压缩(如SQ/RQ)时，可能需要对结果进行重新评分以提高精度
// 返回值: true表示需要重新评分，false表示不需要
func (h *hnsw) shouldRescore() bool {
	// 如果启用了压缩
	if h.compressed.Load() {
		// 检查SQ(标量量化)或RQ(旋转量化)配置
		// 如果启用了但重评分限制为0，则不进行重评分
		if (h.sqConfig.Enabled && h.sqConfig.RescoreLimit == 0) || (h.rqConfig.Enabled && h.rqConfig.RescoreLimit == 0) {
			return false
		}
	}
	// 压缩已启用且未禁用重评分时返回true
	return h.compressed.Load() && !h.doNotRescore
}

// cacheSize 获取缓存中的向量数量
// 根据是否启用压缩返回相应的向量计数
// 返回值: 缓存中的向量总数
func (h *hnsw) cacheSize() int64 {
	var size int64
	if h.compressed.Load() {
		// 压缩模式下使用压缩器统计向量数
		size = h.compressor.CountVectors()
	} else {
		// 未压缩模式下使用普通缓存统计向量数
		size = h.cache.CountVectors()
	}
	return size
}

// acornEnabled 判断ACORN搜索策略是否启用
// ACORN(Adaptive Candidate Recommendation Network)是一种自适应候选推荐网络
// 当允许列表相对较小且ACORN功能启用时使用此策略
// 参数 allowList: 允许列表
// 返回值: true表示启用ACORN策略，false表示不启用
func (h *hnsw) acornEnabled(allowList helpers.AllowList) bool {
	// 如果没有允许列表或ACORN搜索未启用，则不使用ACORN策略
	if allowList == nil || !h.acornSearch.Load() {
		return false
	}

	// 计算缓存大小和允许列表大小的比例
	cacheSize := h.cacheSize()
	allowListSize := allowList.Len()
	
	// 如果允许列表占缓存比例过大，则不使用ACORN策略
	// 因为此时ACORN的优势不明显
	if cacheSize != 0 && float32(allowListSize)/float32(cacheSize) > float32(h.acornFilterRatio) {
		return false
	}

	return true
}

// searchLayerByVectorWithDistancer 使用距离计算器在指定层进行向量搜索
// 这是HNSW搜索的核心实现，支持不同的过滤策略
// 参数:
//   ctx: 上下文
//   queryVector: 查询向量
//   entrypoints: 入口点队列
//   ef: 扩展因子
//   level: 搜索层级
//   allowList: 允许列表
//   compressorDistancer: 压缩距离计算器
// 返回值: 搜索结果队列和错误信息
func (h *hnsw) searchLayerByVectorWithDistancer(ctx context.Context,
	queryVector []float32,
	entrypoints *priorityqueue.Queue[any], ef int, level int,
	allowList helpers.AllowList, compressorDistancer compressionhelpers.CompressorDistancer,
) (*priorityqueue.Queue[any], error,
) {
	// 根据ACORN启用状态选择搜索策略
	if h.acornEnabled(allowList) {
		return h.searchLayerByVectorWithDistancerWithStrategy(ctx, queryVector, entrypoints, ef, level, allowList, compressorDistancer, ACORN)
	}
	// 默认使用扫描策略
	return h.searchLayerByVectorWithDistancerWithStrategy(ctx, queryVector, entrypoints, ef, level, allowList, compressorDistancer, SWEEPING)
}

// searchLayerByVectorWithDistancerWithStrategy 在指定层使用特定策略进行向量搜索
// 这是HNSW层次搜索的具体实现，支持多种过滤策略
// 参数:
//   ctx: 上下文
//   queryVector: 查询向量
//   entrypoints: 入口点队列
//   ef: 扩展因子
//   level: 搜索层级
//   allowList: 允许列表
//   compressorDistancer: 压缩距离计算器
//   strategy: 过滤策略(SWEEPING/ACORN/RRE)
// 返回值: 搜索结果队列和错误信息
func (h *hnsw) searchLayerByVectorWithDistancerWithStrategy(ctx context.Context,
	queryVector []float32,
	entrypoints *priorityqueue.Queue[any], ef int, level int,
	allowList helpers.AllowList, compressorDistancer compressionhelpers.CompressorDistancer,
	strategy FilterStrategy) (*priorityqueue.Queue[any], error,
) {
	// 记录搜索耗时
	start := time.Now()
	defer func() {
		took := time.Since(start)
		helpers.AnnotateSlowQueryLog(ctx, fmt.Sprintf("knn_search_layer_%d_took", level), took)
	}()
	
	// 借用访问列表
	h.pools.visitedListsLock.RLock()
	visited := h.pools.visitedLists.Borrow()
	visitedExp := h.pools.visitedLists.Borrow()
	h.pools.visitedListsLock.RUnlock()

	// 初始化候选队列和结果队列
	candidates := h.pools.pqCandidates.GetMin(ef)
	results := h.pools.pqResults.GetMax(ef)
	
	// 初始化距离计算器
	var floatDistancer distancer.Distancer
	if h.compressed.Load() {
		// 压缩模式使用压缩距离计算器
		if compressorDistancer == nil {
			var returnFn compressionhelpers.ReturnDistancerFn
			compressorDistancer, returnFn = h.compressor.NewDistancer(queryVector)
			defer returnFn()
		}
	} else {
		// 未压缩模式使用浮点距离计算器
		floatDistancer = h.distancerProvider.New(queryVector)
	}

	// 将入口点插入候选队列和结果队列
	h.insertViableEntrypointsAsCandidatesAndResults(entrypoints, candidates,
		results, level, visited, allowList)

	// 判断是否为多向量模式
	isMultivec := h.multivector.Load() && !h.muvera.Load()
	var worstResultDistance float32
	var err error
	
	// 计算当前最差结果距离
	if h.compressed.Load() {
		worstResultDistance, err = h.currentWorstResultDistanceToByte(results, compressorDistancer)
	} else {
		worstResultDistance, err = h.currentWorstResultDistanceToFloat(results, floatDistancer)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "calculate distance of current last result")
	}
	
	// 初始化连接相关变量
	var connectionsReusable []uint64
	var sliceConnectionsReusable *common.VectorUint64Slice
	var slicePendingNextRound *common.VectorUint64Slice
	var slicePendingThisRound *common.VectorUint64Slice

	// 根据策略和允许列表设置搜索参数
	if allowList == nil {
		strategy = SWEEPING
	}
	if strategy == ACORN {
		// ACORN策略需要额外的临时向量切片
		sliceConnectionsReusable = h.pools.tempVectorsUint64.Get(8 * h.maximumConnectionsLayerZero)
		slicePendingNextRound = h.pools.tempVectorsUint64.Get(h.maximumConnectionsLayerZero)
		slicePendingThisRound = h.pools.tempVectorsUint64.Get(h.maximumConnectionsLayerZero)
	} else {
		connectionsReusable = make([]uint64, h.maximumConnectionsLayerZero)
	}

	// 主搜索循环
	for candidates.Len() > 0 {
		// 检查上下文是否取消
		if err := ctx.Err(); err != nil {
			h.pools.visitedListsLock.RLock()
			h.pools.visitedLists.Return(visited)
			h.pools.visitedListsLock.RUnlock()

			helpers.AnnotateSlowQueryLog(ctx, "context_error", "knn_search_layer")
			return nil, err
		}
		
		// 获取候选节点
		var dist float32
		candidate := candidates.Pop()
		dist = candidate.Dist

		// 提前终止条件：如果当前距离已经超过最差结果且结果已满
		if dist > worstResultDistance && results.Len() >= ef {
			break
		}

		// 获取候选节点（带读锁）
		h.shardedNodeLocks.RLock(candidate.ID)
		candidateNode := h.nodes[candidate.ID]
		h.shardedNodeLocks.RUnlock(candidate.ID)

		// 跳过已被删除的节点
		if candidateNode == nil {
			// 可能是刚附加了墓碑标记并正在清理的节点
			continue
		}

		// 锁定节点进行处理
		candidateNode.Lock()
		if candidateNode.level < level {
			// 节点层级可能因删除重新分配而降级，
			// 但指向它的连接尚未清理。这种情况下
			// 节点在此层级没有传出连接，必须丢弃。
			candidateNode.Unlock()
			continue
		}

		// 处理节点连接
		func() {
			// 确保即使在访问连接时发生panic也能解锁节点
			defer func() {
				if err := recover(); err != nil {
					candidateNode.Unlock()
					panic(errors.Errorf("shard: %s, collection: %s, vectorIndex: %s, panic: %v", h.shardName, h.className, h.id, err))
				}
			}()

			if strategy != ACORN {
				// 非ACORN策略：直接复制连接
				if candidateNode.connections.LenAtLayer(uint8(level)) > h.maximumConnectionsLayerZero {
					// 如何可能出现连接数超过允许的最大值？
					// 这在v1.12.0之前的版本中由于bug可能发生：
					// https://github.com/weaviate/weaviate/issues/1868
					//
					// 结果是这个切片的长度完全不可预测，我们不能再从池中获取。
					// 相反，我们需要回退到分配新切片。
					//
					// 这是在以下问题中发现的：
					// https://github.com/weaviate/weaviate/issues/1897
					connectionsReusable = make([]uint64, candidateNode.connections.LenAtLayer(uint8(level)))
				} else {
					connectionsReusable = connectionsReusable[:candidateNode.connections.LenAtLayer(uint8(level))]
				}
				connectionsReusable = candidateNode.connections.CopyLayer(connectionsReusable, uint8(level))
			} else {
				// ACORN策略：智能选择连接
				connectionsReusable = sliceConnectionsReusable.Slice
				pendingNextRound := slicePendingNextRound.Slice
				pendingThisRound := slicePendingThisRound.Slice

				realLen := 0
				index := 0

				pendingNextRound = pendingNextRound[:candidateNode.connections.LenAtLayer(uint8(level))]
				pendingNextRound = candidateNode.connections.CopyLayer(pendingNextRound, uint8(level))
				hop := 1
				maxHops := 2
				
				// 多跳探索连接
				for hop <= maxHops && realLen < 8*h.maximumConnectionsLayerZero && len(pendingNextRound) > 0 {
					if cap(pendingThisRound) >= len(pendingNextRound) {
						pendingThisRound = pendingThisRound[:len(pendingNextRound)]
					} else {
						pendingThisRound = make([]uint64, len(pendingNextRound))
						slicePendingThisRound.Slice = pendingThisRound
					}
					copy(pendingThisRound, pendingNextRound)
					pendingNextRound = pendingNextRound[:0]
					
					for index < len(pendingThisRound) && realLen < 8*h.maximumConnectionsLayerZero {
						nodeId := pendingThisRound[index]
						index++
						
						if ok := visited.Visited(nodeId); ok {
							// 跳过已访问的邻居
							continue
						}
						
						if !visitedExp.Visited(nodeId) {
							// 检查是否在允许列表中
							if !isMultivec {
								if allowList.Contains(nodeId) {
									connectionsReusable[realLen] = nodeId
									realLen++
									visitedExp.Visit(nodeId)
									continue
								}
							} else {
								var docID uint64
								if h.compressed.Load() {
									docID, _ = h.compressor.GetKeys(nodeId)
								} else {
									docID, _ = h.cache.GetKeys(nodeId)
								}
								if allowList.Contains(docID) {
									connectionsReusable[realLen] = nodeId
									realLen++
									visitedExp.Visit(nodeId)
									continue
								}
							}
						} else {
							continue
						}
						
						visitedExp.Visit(nodeId)

						// 获取邻居节点
						h.RLock()
						h.shardedNodeLocks.RLock(nodeId)
						node := h.nodes[nodeId]
						h.shardedNodeLocks.RUnlock(nodeId)
						h.RUnlock()
						
						if node == nil {
							continue
						}
						
						// 遍历邻居的连接
						iterator := node.connections.ElementIterator(uint8(level))
						for iterator.Next() {
							_, expId := iterator.Current()
							if visitedExp.Visited(expId) {
								continue
							}
							if visited.Visited(expId) {
								continue
							}

							if realLen >= 8*h.maximumConnectionsLayerZero {
								break
							}

							// 检查扩展节点是否在允许列表中
							if !isMultivec {
								if allowList.Contains(expId) {
									visitedExp.Visit(expId)
									connectionsReusable[realLen] = expId
									realLen++
								} else if hop < maxHops {
									visitedExp.Visit(expId)
									pendingNextRound = append(pendingNextRound, expId)
								}
							} else {
								var docID uint64
								if h.compressed.Load() {
									docID, _ = h.compressor.GetKeys(expId)
								} else {
									docID, _ = h.cache.GetKeys(expId)
								}
								if allowList.Contains(docID) {
									visitedExp.Visit(expId)
									connectionsReusable[realLen] = expId
									realLen++
								} else if hop < maxHops {
									visitedExp.Visit(expId)
									pendingNextRound = append(pendingNextRound, expId)
								}
							}
						}
					}
					hop++
				}
				slicePendingNextRound.Slice = pendingNextRound
				connectionsReusable = connectionsReusable[:realLen]
			}
		}()

		candidateNode.Unlock()

		// 处理候选连接
		for _, neighborID := range connectionsReusable {
			if ok := visited.Visited(neighborID); ok {
				// 跳过已访问的邻居
				continue
			}

			// 标记为已访问
			visited.Visit(neighborID)

			// RRE策略在第0层的特殊过滤
			if strategy == RRE && level == 0 {
				if isMultivec {
					var docID uint64
					if h.compressed.Load() {
						docID, _ = h.compressor.GetKeys(neighborID)
					} else {
						docID, _ = h.cache.GetKeys(neighborID)
					}
					if !allowList.Contains(docID) {
						continue
					}
				} else if !allowList.Contains(neighborID) {
					continue
				}
			}
			
			// 计算到邻居的距离
			var distance float32
			var err error
			if h.compressed.Load() {
				distance, err = compressorDistancer.DistanceToNode(neighborID)
			} else {
				distance, err = h.distanceToFloatNode(floatDistancer, neighborID)
			}
			if err != nil {
				var e storobj.ErrNotFound
				if errors.As(err, &e) {
					h.handleDeletedNode(e.DocID, "searchLayerByVectorWithDistancer")
					continue
				} else {
					h.pools.visitedListsLock.RLock()
					h.pools.visitedLists.Return(visited)
					h.pools.visitedLists.Return(visitedExp)
					h.pools.visitedListsLock.RUnlock()
					return nil, errors.Wrap(err, "calculate distance between candidate and query")
				}
			}

			// 更新候选队列和结果队列
			if distance < worstResultDistance || results.Len() < ef {
				candidates.Insert(neighborID, distance)
				
				// 第0层的允许列表过滤
				if strategy == SWEEPING && level == 0 && allowList != nil {
					// 我们在包含实际候选者的最低层，
					// 并且有允许列表（即用户可能设置了某种过滤器限制此搜索）。
					// 因此我们必须忽略不在列表上的项目
					if isMultivec {
						var docID uint64
						if h.compressed.Load() {
							docID, _ = h.compressor.GetKeys(neighborID)
						} else {
							docID, _ = h.cache.GetKeys(neighborID)
						}
						if !allowList.Contains(docID) {
							continue
						}
					} else if !allowList.Contains(neighborID) {
						continue
					}
				}

				// 跳过有墓碑标记的节点
				if h.hasTombstone(neighborID) {
					continue
				}

				results.Insert(neighborID, distance)

				// 预取下一个候选节点
				if h.compressed.Load() {
					h.compressor.Prefetch(candidates.Top().ID)
				} else {
					h.cache.Prefetch(candidates.Top().ID)
				}

				// 维护结果队列大小
				if results.Len() > ef {
					results.Pop()
				}

				// 更新最差距离
				if results.Len() > 0 {
					worstResultDistance = results.Top().Dist
				}
			}
		}
	}

	// 归还ACORN策略使用的临时资源
	if strategy == ACORN {
		h.pools.tempVectorsUint64.Put(sliceConnectionsReusable)
		h.pools.tempVectorsUint64.Put(slicePendingNextRound)
		h.pools.tempVectorsUint64.Put(slicePendingThisRound)
	}

	// 归还资源
	h.pools.pqCandidates.Put(candidates)

	h.pools.visitedListsLock.RLock()
	h.pools.visitedLists.Return(visited)
	h.pools.visitedLists.Return(visitedExp)
	h.pools.visitedListsLock.RUnlock()

	return results, nil
}

// insertViableEntrypointsAsCandidatesAndResults 将有效的入口点插入候选队列和结果队列
// 在搜索开始时初始化搜索过程
// 参数:
//   entrypoints: 入口点队列
//   candidates: 候选队列
//   results: 结果队列
//   level: 当前搜索层级
//   visitedList: 已访问节点列表
//   allowList: 允许列表
func (h *hnsw) insertViableEntrypointsAsCandidatesAndResults(
	entrypoints, candidates, results *priorityqueue.Queue[any], level int,
	visitedList visited.ListSet, allowList helpers.AllowList,
) {
	// 判断是否为多向量模式
	isMultivec := h.multivector.Load() && !h.muvera.Load()
	
	// 处理所有入口点
	for entrypoints.Len() > 0 {
		ep := entrypoints.Pop()
		visitedList.Visit(ep.ID)
		candidates.Insert(ep.ID, ep.Dist)
		
		// 第0层的允许列表过滤
		if level == 0 && allowList != nil {
			// 我们在包含实际候选者的最低层，
			// 并且有允许列表（即用户可能设置了某种过滤器限制此搜索）。
			// 因此我们必须忽略不在列表上的项目
			if isMultivec {
				var docID uint64
				if h.compressed.Load() {
					docID, _ = h.compressor.GetKeys(ep.ID)
				} else {
					docID, _ = h.cache.GetKeys(ep.ID)
				}
				if !allowList.Contains(docID) {
					continue
				}
			} else if !allowList.Contains(ep.ID) {
				continue
			}
		}

		// 跳过有墓碑标记的节点
		if h.hasTombstone(ep.ID) {
			continue
		}

		// 将有效入口点加入结果队列
		results.Insert(ep.ID, ep.Dist)
	}
}

// currentWorstResultDistanceToFloat 计算当前最差结果到浮点节点的距离
// 用于确定搜索过程中的距离边界
// 参数:
//   results: 当前结果队列
//   distancer: 距离计算器
// 返回值: 最差距离值和错误信息
func (h *hnsw) currentWorstResultDistanceToFloat(results *priorityqueue.Queue[any],
	distancer distancer.Distancer,
) (float32, error) {
	if results.Len() > 0 {
		id := results.Top().ID

		d, err := h.distanceToFloatNode(distancer, id)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID, "currentWorstResultDistanceToFloat")
				return math.MaxFloat32, nil
			}
			return 0, errors.Wrap(err, "calculated distance between worst result and query")
		}

		return d, nil
	} else {
		// 如果入口点（从更高层获得的）不匹配允许列表，结果列表为空。
		// 在这种情况下，我们可以将最差距离设置为任意大的数字，
		// 这样任何（允许的）候选者在比较时都会有更小的距离
		return math.MaxFloat32, nil
	}
}

// currentWorstResultDistanceToByte 计算当前最差结果到字节节点的距离
// 用于压缩模式下的距离计算
// 参数:
//   results: 当前结果队列
//   distancer: 压缩距离计算器
// 返回值: 最差距离值和错误信息
func (h *hnsw) currentWorstResultDistanceToByte(results *priorityqueue.Queue[any],
	distancer compressionhelpers.CompressorDistancer,
) (float32, error) {
	if results.Len() > 0 {
		item := results.Top()
		if item.Dist != 0 {
			return item.Dist, nil
		}
		id := item.ID
		d, err := distancer.DistanceToNode(id)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID, "currentWorstResultDistanceToByte")
				return math.MaxFloat32, nil
			}
			return 0, errors.Wrap(err,
				"calculated distance between worst result and query")
		}

		return d, nil
	} else {
		// 如果入口点（从更高层获得的）不匹配允许列表，结果列表为空。
		// 在这种情况下，我们可以将最差距离设置为任意大的数字，
		// 这样任何（允许的）候选者在比较时都会有更小的距离
		return math.MaxFloat32, nil
	}
}

// distanceFromBytesToFloatNodeWithView 通过视图计算字节节点到浮点节点的距离
// 用于重评分过程中获取精确距离
// 参数:
//   ctx: 上下文
//   concreteDistancer: 具体的距离计算器
//   nodeID: 节点ID
//   view: 存储视图
// 返回值: 计算得到的距离和错误信息
func (h *hnsw) distanceFromBytesToFloatNodeWithView(ctx context.Context, concreteDistancer compressionhelpers.CompressorDistancer, nodeID uint64, view common.BucketView) (float32, error) {
	// 获取临时向量切片
	slice := h.pools.tempVectors.Get(int(h.dims))
	defer h.pools.tempVectors.Put(slice)
	var vec []float32
	var err error
	
	// 根据模式获取向量
	if h.muvera.Load() || !h.multivector.Load() {
		vec, err = h.TempVectorForIDWithViewThunk(ctx, nodeID, slice, view)
	} else {
		docID, relativeID := h.cache.GetKeys(nodeID)
		vecs, err := h.TempMultiVectorForIDWithViewThunk(ctx, docID, slice, view)
		if err != nil {
			return 0, err
		} else if len(vecs) <= int(relativeID) {
			return 0, errors.Errorf("relativeID %d is out of bounds for docID %d", relativeID, docID)
		}
		vec = vecs[relativeID]
	}
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID, "distanceFromBytesToFloatNodeWithView")
			return 0, err
		}
		// 不是可恢复的类型错误，返回错误
		return 0, errors.Wrapf(err, "get vector of docID %d", nodeID)
	}
	// 就地归一化，因为vec指向将在函数结束后返回的池化切片
	// 这避免了在重评分期间为每个向量分配新切片
	h.normalizeVecInPlace(vec)
	return concreteDistancer.DistanceToFloat(vec)
}

// distanceToFloatNode 计算到浮点节点的距离
// 参数:
//   distancer: 距离计算器
//   nodeID: 目标节点ID
// 返回值: 计算得到的距离和错误信息
func (h *hnsw) distanceToFloatNode(distancer distancer.Distancer, nodeID uint64) (float32, error) {
	// 获取候选向量
	candidateVec, err := h.vectorForID(context.Background(), nodeID)
	if err != nil {
		return 0, err
	}

	// 计算距离
	dist, err := distancer.Distance(candidateVec)
	if err != nil {
		return 0, errors.Wrap(err, "calculate distance between candidate and query")
	}

	return dist, nil
}

// handleDeletedNode 处理已删除的节点
// 当发现没有墓碑标记的已删除节点时，为其添加墓碑标记以便清理
// 参数:
//   docID: 文档ID
//   operation: 操作名称（用于日志记录）
func (h *hnsw) handleDeletedNode(docID uint64, operation string) {
	if h.hasTombstone(docID) {
		// 无需操作，此节点已有墓碑标记，将在下次删除周期中清理
		return
	}

	// 添加墓碑标记
	h.addTombstone(docID)
	h.metrics.AddUnexpectedTombstone(operation)
	h.logger.WithField("action", "attach_tombstone_to_deleted_node").
		WithField("node_id", docID).
		Debugf("found a deleted node (%d) without a tombstone, "+
			"tombstone was added", docID)
}

// knnSearchByVector K近邻向量搜索主函数
// 实现完整的HNSW层次搜索算法
// 参数:
//   ctx: 上下文
//   searchVec: 查询向量
//   k: 需要返回的最近邻数量
//   ef: 扩展因子
//   allowList: 允许列表
// 返回值: 结果ID列表、距离列表和错误信息
func (h *hnsw) knnSearchByVector(ctx context.Context, searchVec []float32, k int,
	ef int, allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	// 检查索引是否为空
	if h.isEmpty() {
		return nil, nil, nil
	}

	// 验证k值有效性
	if k < 0 {
		return nil, nil, fmt.Errorf("k must be greater than zero")
	}

	// 获取全局入口点和最大层级（带读锁）
	h.RLock()
	entryPointID := h.entryPointID
	maxLayer := h.currentMaximumLayer
	h.RUnlock()

	// 初始化压缩距离计算器
	var compressorDistancer compressionhelpers.CompressorDistancer
	if h.compressed.Load() {
		var returnFn compressionhelpers.ReturnDistancerFn
		compressorDistancer, returnFn = h.compressor.NewDistancer(searchVec)
		defer returnFn()
	}
	
	// 计算入口点到查询向量的距离
	entryPointDistance, err := h.distToNode(compressorDistancer, entryPointID, searchVec)
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID, "knnSearchByVector")
			return nil, nil, fmt.Errorf("entrypoint was deleted in the object store, " +
				"it has been flagged for cleanup and should be fixed in the next cleanup cycle")
		}
		return nil, nil, errors.Wrap(err, "knn search: distance between entrypoint and query node")
	}

	// 自上而下进行层次搜索（停止在第1层，不是第0层！）
	for level := maxLayer; level >= 1; level-- {
		eps := priorityqueue.NewMin[any](10)
		eps.Insert(entryPointID, entryPointDistance)

		// 在当前层搜索
		res, err := h.searchLayerByVectorWithDistancer(ctx, searchVec, eps, 1, level, nil, compressorDistancer)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "knn search: search layer at level %d", level)
		}

		// 可能出现我们在该特定层找不到更好的入口点的情况，
		// 所以我们保留之前的入口点（即来自前一层甚至是主要入口点的入口点）
		//
		// 但是，如果我们确实有结果，任何候选者如果不是nil（未删除），
		// 且不在维护中就是可行的候选者
		for res.Len() > 0 {
			cand := res.Pop()
			n := h.nodeByID(cand.ID)
			if n == nil {
				// 我们在结果中找到了一个nil节点。这意味着它已被删除，
				// 但未正确清理。确保为此节点添加墓碑标记，
				// 以便在下一个周期中清理。
				if err := h.addTombstone(cand.ID); err != nil {
					return nil, nil, err
				}

				// 跳过nil节点，因为它不能作为有效的入口点
				continue
			}

			if !n.isUnderMaintenance() {
				entryPointID = cand.ID
				entryPointDistance = cand.Dist
				break
			}

			// 如果我们遍历循环而没有找到单个合适的节点，
			// 我们简单地坚持原始入口点，即全局入口点
		}

		h.pools.pqResults.Put(res)
	}

	// 在第0层进行最终搜索
	eps := priorityqueue.NewMin[any](10)
	eps.Insert(entryPointID, entryPointDistance)
	var strategy FilterStrategy
	
	// 确定过滤策略
	h.shardedNodeLocks.RLock(entryPointID)
	entryPointNode := h.nodes[entryPointID]
	h.shardedNodeLocks.RUnlock(entryPointID)
	useAcorn := h.acornEnabled(allowList)
	isMultivec := h.multivector.Load() && !h.muvera.Load()
	
	if useAcorn {
		if entryPointNode == nil {
			strategy = RRE
		} else {
			counter := float32(0)
			entryPointNode.Lock()
			if entryPointNode.connections.Layers() < 1 {
				strategy = ACORN
			} else {
				iterator := entryPointNode.connections.ElementIterator(0)
				for iterator.Next() {
					_, value := iterator.Current()
					if isMultivec {
						if h.compressed.Load() {
							value, _ = h.compressor.GetKeys(value)
						} else {
							value, _ = h.cache.GetKeys(value)
						}
					}
					if allowList.Contains(value) {
						counter++
					}
				}
				entryPointNode.Unlock()
				if counter/float32(h.nodes[entryPointID].connections.LenAtLayer(0)) > float32(h.acornFilterRatio) {
					strategy = RRE
				} else {
					strategy = ACORN
				}
			}
		}
	} else {
		strategy = SWEEPING
	}

	// 如果有允许列表且使用ACORN策略，初始化多个种子点
	if allowList != nil && useAcorn {
		seeds := 10
		it := allowList.Iterator()
		idx, ok := it.Next()
		h.shardedNodeLocks.RLockAll()
		for seeds > 0 {
			if !isMultivec {
				for ok && (h.nodes[idx] == nil || h.hasTombstone(idx)) {
					idx, ok = it.Next()
				}
			} else {
				_, exists := h.docIDVectors[idx]
				for ok && !exists {
					idx, ok = it.Next()
					_, exists = h.docIDVectors[idx]
				}
			}

			if !ok || !allowList.Contains(idx) {
				break
			}

			entryPointDistance, _ := h.distToNode(compressorDistancer, idx, searchVec)
			eps.Insert(idx, entryPointDistance)
			idx, ok = it.Next()
			seeds--
		}
		h.shardedNodeLocks.RUnlockAll()
	}
	
	// 执行第0层搜索
	res, err := h.searchLayerByVectorWithDistancerWithStrategy(ctx, searchVec, eps, ef, 0, allowList, compressorDistancer, strategy)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "knn search: search layer at level %d", 0)
	}

	// 重评分阶段
	beforeRescore := time.Now()
	if h.shouldRescore() && !h.multivector.Load() {
		if err := h.rescore(ctx, res, k, compressorDistancer); err != nil {
			helpers.AnnotateSlowQueryLog(ctx, "context_error", "knn_search_rescore")
			took := time.Since(beforeRescore)
			helpers.AnnotateSlowQueryLog(ctx, "knn_search_rescore_took", took)
			return nil, nil, fmt.Errorf("knn search:  %w", err)
		}
		took := time.Since(beforeRescore)
		helpers.AnnotateSlowQueryLog(ctx, "knn_search_rescore_took", took)
	}

	// 调整结果数量
	if !h.multivector.Load() {
		for res.Len() > k {
			res.Pop()
		}
	}
	
	// 准备返回结果
	ids := make([]uint64, res.Len())
	dists := make([]float32, res.Len())

	// 结果是逆序的，我们需要在呈现给用户之前翻转顺序！
	i := len(ids) - 1
	for res.Len() > 0 {
		res := res.Pop()
		ids[i] = res.ID
		dists[i] = res.Dist
		i--
	}
	
	h.pools.pqResults.Put(res)
	return ids, dists, nil
}

func (h *hnsw) knnSearchByMultiVector(ctx context.Context, queryVectors [][]float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	kPrime := k
	candidateSet := make(map[uint64]struct{})
	for _, vec := range queryVectors {
		ids, _, err := h.knnSearchByVector(ctx, vec, kPrime, h.searchTimeEF(kPrime), allowList)
		if err != nil {
			return nil, nil, err
		}
		for _, id := range ids {
			var docId uint64
			if !h.compressed.Load() {
				docId, _ = h.cache.GetKeys(id)
			} else {
				docId, _ = h.compressor.GetKeys(id)
			}
			candidateSet[docId] = struct{}{}
		}
	}
	return h.computeLateInteraction(queryVectors, k, candidateSet)
}

func (h *hnsw) computeLateInteraction(queryVectors [][]float32, k int, candidateSet map[uint64]struct{}) ([]uint64, []float32, error) {
	resultsQueue := priorityqueue.NewMax[any](1)
	for docID := range candidateSet {
		sim, err := h.computeScore(queryVectors, docID)
		if err != nil {
			return nil, nil, err
		}
		resultsQueue.Insert(docID, sim)
		if resultsQueue.Len() > k {
			resultsQueue.Pop()
		}
	}

	distances := make([]float32, resultsQueue.Len())
	ids := make([]uint64, resultsQueue.Len())

	i := len(ids) - 1
	for resultsQueue.Len() > 0 {
		element := resultsQueue.Pop()
		ids[i] = element.ID
		distances[i] = element.Dist
		i--
	}

	return ids, distances, nil
}

func (h *hnsw) computeScore(searchVecs [][]float32, docID uint64) (float32, error) {
	h.RLock()
	vecIDs := h.docIDVectors[docID]
	h.RUnlock()
	var docVecs [][]float32
	if h.compressed.Load() {
		slice := h.pools.tempVectors.Get(int(h.dims))
		var err error
		docVecs, err = h.TempMultiVectorForIDThunk(context.Background(), docID, slice)
		if err != nil {
			return 0.0, errors.Wrap(err, "get vector for docID")
		}
		h.pools.tempVectors.Put(slice)
	} else {
		if !h.muvera.Load() {
			var errs []error
			docVecs, errs = h.multiVectorForID(context.Background(), vecIDs)
			for _, err := range errs {
				if err != nil {
					return 0.0, errors.Wrap(err, "get vector for docID")
				}
			}
		} else {
			var err error
			docVecs, err = h.cache.GetDoc(context.Background(), docID)
			if err != nil {
				return 0.0, errors.Wrap(err, "get muvera vector for docID")
			}
		}
	}

	similarity := float32(0.0)

	var distancer distancer.Distancer
	for _, searchVec := range searchVecs {
		maxSim := float32(math.MaxFloat32)
		distancer = h.multiDistancerProvider.New(searchVec)

		for _, docVec := range docVecs {
			dist, err := distancer.Distance(docVec)
			if err != nil {
				return 0.0, errors.Wrap(err, "calculate distance between candidate and query")
			}
			if dist < maxSim {
				maxSim = dist
			}
		}

		similarity += maxSim
	}

	return similarity, nil
}

func (h *hnsw) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	queryVector = h.normalizeVec(queryVector)
	if h.compressed.Load() {
		dist, returnFn := h.compressor.NewDistancer(queryVector)
		f := func(nodeID uint64) (float32, error) {
			if int(nodeID) > len(h.nodes) {
				return -1, fmt.Errorf("node %v is larger than the cache size %v", nodeID, len(h.nodes))
			}

			return dist.DistanceToNode(nodeID)
		}
		return common.QueryVectorDistancer{DistanceFunc: f, CloseFunc: returnFn}

	} else {
		distancer := h.distancerProvider.New(queryVector)
		f := func(nodeID uint64) (float32, error) {
			if int(nodeID) > len(h.nodes) {
				return -1, fmt.Errorf("node %v is larger than the cache size %v", nodeID, len(h.nodes))
			}
			return h.distanceToFloatNode(distancer, nodeID)
		}
		return common.QueryVectorDistancer{DistanceFunc: f}
	}
}

func (h *hnsw) QueryMultiVectorDistancer(queryVector [][]float32) common.QueryVectorDistancer {
	queryVector = h.normalizeVecs(queryVector)
	f := func(docID uint64) (float32, error) {
		h.RLock()
		_, ok := h.docIDVectors[docID]
		h.RUnlock()
		if !ok {
			return -1, fmt.Errorf("docID %v is not in the vector index", docID)
		}
		return h.computeScore(queryVector, docID)
	}
	return common.QueryVectorDistancer{DistanceFunc: f}
}

func (h *hnsw) rescore(ctx context.Context, res *priorityqueue.Queue[any], k int, compressorDistancer compressionhelpers.CompressorDistancer) error {
	if h.sqConfig.Enabled && h.sqConfig.RescoreLimit >= k {
		for res.Len() > h.sqConfig.RescoreLimit {
			res.Pop()
		}
	}
	if h.rqConfig.Enabled && h.rqConfig.RescoreLimit >= k {
		for res.Len() > h.rqConfig.RescoreLimit {
			res.Pop()
		}
	}
	ids := make([]uint64, res.Len())
	i := len(ids) - 1
	for res.Len() > 0 {
		res := res.Pop()
		ids[i] = res.ID
		i--
	}
	res.Reset()

	// Get a consistent view once for all vector lookups to reduce lock contention
	view := h.GetViewThunk()
	defer view.ReleaseView()

	mu := sync.Mutex{} // protect res
	addID := func(id uint64, dist float32) {
		mu.Lock()
		defer mu.Unlock()

		res.Insert(id, dist)
		if res.Len() > k {
			res.Pop()
		}
	}

	eg := enterrors.NewErrorGroupWrapper(h.logger)
	for workerID := 0; workerID < h.rescoreConcurrency; workerID++ {
		workerID := workerID

		eg.Go(func() error {
			for idPos := workerID; idPos < len(ids); idPos += h.rescoreConcurrency {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("rescore: %w", err)
				}

				id := ids[idPos]
				dist, err := h.distanceFromBytesToFloatNodeWithView(ctx, compressorDistancer, id, view)
				if err == nil {
					addID(id, dist)
				} else {
					h.logger.
						WithField("action", "rescore").
						WithField("id", h.id).
						WithField("class", h.className).
						WithField("shard", h.shardName).
						WithError(err).
						Warnf("could not rescore node %d", id)
				}
			}
			return nil
		}, h.logger)
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func newSearchByDistParams(maxLimit int64) *searchByDistParams {
	initialOffset := 0
	initialLimit := DefaultSearchByDistInitialLimit

	return &searchByDistParams{
		offset:             initialOffset,
		limit:              initialLimit,
		totalLimit:         initialOffset + initialLimit,
		maximumSearchLimit: maxLimit,
	}
}

const (
	// DefaultSearchByDistInitialLimit :
	// the initial limit of 100 here is an
	// arbitrary decision, and can be tuned
	// as needed
	DefaultSearchByDistInitialLimit = 100

	// DefaultSearchByDistLimitMultiplier :
	// the decision to increase the limit in
	// multiples of 10 here is an arbitrary
	// decision, and can be tuned as needed
	DefaultSearchByDistLimitMultiplier = 10
)

type searchByDistParams struct {
	offset             int
	limit              int
	totalLimit         int
	maximumSearchLimit int64
}

func (params *searchByDistParams) offsetCapacity(ids []uint64) int {
	var offsetCap int
	if params.offset < len(ids) {
		offsetCap = params.offset
	} else {
		offsetCap = len(ids)
	}

	return offsetCap
}

func (params *searchByDistParams) totalLimitCapacity(ids []uint64) int {
	var totalLimitCap int
	if params.totalLimit < len(ids) {
		totalLimitCap = params.totalLimit
	} else {
		totalLimitCap = len(ids)
	}

	return totalLimitCap
}

func (params *searchByDistParams) iterate() {
	params.offset = params.totalLimit
	params.limit *= DefaultSearchByDistLimitMultiplier
	params.totalLimit = params.offset + params.limit
}

func (params *searchByDistParams) maxLimitReached() bool {
	if params.maximumSearchLimit < 0 {
		return false
	}

	return int64(params.totalLimit) > params.maximumSearchLimit
}

func searchByVectorDistance[T dto.Embedding](ctx context.Context, vector T,
	targetDistance float32, maxLimit int64,
	allowList helpers.AllowList,
	searchByVector func(context.Context, T, int, helpers.AllowList) ([]uint64, []float32, error),
	logger logrus.FieldLogger,
) ([]uint64, []float32, error) {
	var (
		searchParams = newSearchByDistParams(maxLimit)

		resultIDs  []uint64
		resultDist []float32
	)

	recursiveSearch := func() (bool, error) {
		shouldContinue := false

		ids, dist, err := searchByVector(ctx, vector, searchParams.totalLimit, allowList)
		if err != nil {
			return false, errors.Wrap(err, "vector search")
		}

		// ensures the indexers aren't out of range
		offsetCap := searchParams.offsetCapacity(ids)
		totalLimitCap := searchParams.totalLimitCapacity(ids)

		ids, dist = ids[offsetCap:totalLimitCap], dist[offsetCap:totalLimitCap]

		if len(ids) == 0 {
			return false, nil
		}

		lastFound := dist[len(dist)-1]
		shouldContinue = lastFound <= targetDistance

		for i := range ids {
			if aboveThresh := dist[i] <= targetDistance; aboveThresh ||
				floatcomp.InDelta(float64(dist[i]), float64(targetDistance), 1e-6) {
				resultIDs = append(resultIDs, ids[i])
				resultDist = append(resultDist, dist[i])
			} else {
				// as soon as we encounter a certainty which
				// is below threshold, we can stop searching
				break
			}
		}

		return shouldContinue, nil
	}

	shouldContinue, err := recursiveSearch()
	if err != nil {
		return nil, nil, err
	}

	for shouldContinue {
		searchParams.iterate()
		if searchParams.maxLimitReached() {
			logger.
				WithField("action", "unlimited_vector_search").
				Warnf("maximum search limit of %d results has been reached",
					searchParams.maximumSearchLimit)
			break
		}

		shouldContinue, err = recursiveSearch()
		if err != nil {
			return nil, nil, err
		}
	}

	return resultIDs, resultDist, nil
}
