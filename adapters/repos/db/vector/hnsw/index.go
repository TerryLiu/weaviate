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
	"io"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// hnsw 结构体定义了 HNSW 向量索引的核心数据结构
// HNSW (Hierarchical Navigable Small World) 是一种高效的近似最近邻搜索算法
type hnsw struct {
	// global lock to prevent concurrent map read/write, etc.
	// 全局读写锁，用于防止并发的 map 读写等操作
	sync.RWMutex

	// certain operations related to deleting, such as finding a new entrypoint
	// can only run sequentially, this separate lock helps assuring this without
	// blocking the general usage of the hnsw index
	// 删除相关操作的专用锁，如寻找新的入口点等操作必须串行执行
	// 这个独立的锁确保了这一点，而不会阻塞 HNSW 索引的一般使用
	deleteLock *sync.Mutex

	// 墓碑锁，用于墓碑清理操作的同步
tombstoneLock *sync.RWMutex

	// prevents tombstones cleanup to be performed in parallel with index reset operation
	// 防止墓碑清理与索引重置操作并行执行
	resetLock *sync.RWMutex
	// indicates whether reset operation occurred or not - if so tombstones cleanup method
	// is aborted as it makes no sense anymore
	// 重置操作的上下文 - 如果发生了重置操作，墓碑清理方法将被中止
	// 因为此时清理已无意义
	resetCtx       context.Context
	resetCtxCancel context.CancelFunc

	// indicates the index is shutting down
	// 指示索引正在关闭
	shutdownCtx       context.Context
	shutdownCtxCancel context.CancelFunc

	// make sure the very first insert happens just once, otherwise we
	// accidentally overwrite previous entrypoints on parallel imports on an
	// empty graph
	// 确保第一次插入只发生一次，否则在空图上并行导入时会意外覆盖之前的入口点
	initialInsertOnce *sync.Once

	// Each node should not have more edges than this number
	// 每个节点的最大连接数限制
	maximumConnections int

	// Nodes in the lowest level have a separate (usually higher) max connection
	// limit
	// 最底层节点有单独的（通常更高的）最大连接限制
	maximumConnectionsLayerZero int

	// the current maximum can be smaller than the configured maximum because of
	// the exponentially decaying layer function. The initial entry is started at
	// layer 0, but this has the chance to grow with every subsequent entry
	// 当前最大层数可能小于配置的最大值，这是由于指数衰减的层函数
	// 初始入口点从第0层开始，但随着后续条目的增加有机会增长
	currentMaximumLayer int

	// this is a point on the highest level, if we insert a new point with a
	// higher level it will become the new entry point. Note tat the level of
	// this point is always currentMaximumLayer
	// 这是最高层的一个点，如果我们插入一个更高级别的新点，它将成为新的入口点
	// 注意这个点的层级始终是 currentMaximumLayer
	entryPointID uint64

	// ef parameter used in construction phases, should be higher than ef during querying
	// 构建阶段使用的 ef 参数，应该比查询时的 ef 更高
	efConstruction int

	// ef at search time
	// 查询时的 ef 参数
	ef int64

	// only used if ef=-1
	// 仅在 ef=-1 时使用
	efMin    int64
	efMax    int64
	efFactor int64

	// on filtered searches with less than n elements, perform flat search
	// 在过滤搜索中元素少于 n 个时，执行平面搜索
	flatSearchCutoff      int64
	flatSearchConcurrency int

	// 层级标准化因子
	levelNormalizer float64

	// 节点数组
	nodes []*vertex

	// 根据ID获取向量的函数
	vectorForID                       common.VectorForID[float32]
	// 临时多向量获取函数
	TempMultiVectorForIDThunk         common.TempVectorForID[[]float32]
	// 获取存储视图的函数
	GetViewThunk                      common.GetViewThunk
	// 带视图的临时向量获取函数
	TempVectorForIDWithViewThunk      common.TempVectorForIDWithView[float32]
	// 带视图的临时多向量获取函数
	TempMultiVectorForIDWithViewThunk common.TempVectorForIDWithView[[]float32]
	// 多向量获取函数
	multiVectorForID                  common.MultiVectorForID
	// 维度跟踪一次性初始化
	trackDimensionsOnce               sync.Once
	// Muvera编码器一次性初始化
	trackMuveraOnce                   sync.Once
	// RQ压缩一次性初始化
	trackRQOnce                       sync.Once
	// 向量维度
	dims                              int32

	// 向量缓存
	cache               cache.Cache[float32]
	// 是否等待缓存预填充完成
	waitForCachePrefill bool
	// 缓存是否已预填充
	cachePrefilled      atomic.Bool

	// 提交日志，用于持久化索引操作
	commitLog CommitLogger

	// a lookup of current tombstones (i.e. nodes that have received a tombstone,
	// but have not been cleaned up yet) Cleanup is the process of removal of all
	// outgoing edges to the tombstone as well as deleting the tombstone itself.
	// This process should happen periodically.
	// 当前墓碑的查找表（即已收到墓碑标记但尚未清理的节点）
	// 清理过程包括移除所有指向墓碑的出边以及删除墓碑本身
	// 这个过程应该定期执行
	tombstones map[uint64]struct{}

	// 墓碑清理回调控制器
	tombstoneCleanupCallbackCtrl cyclemanager.CycleCallbackCtrl

	// // for distributed spike, can be used to call a insertExternal on a different graph
	// insertHook func(node, targetLevel int, neighborsAtLevel map[int][]uint32)

	// 索引唯一标识符
	id       string
	// 索引根路径
	rootPath string

	// 日志记录器
	logger                 logrus.FieldLogger
	// 距离计算提供者
	distancerProvider      distancer.Provider
	// 多向量距离计算提供者
	multiDistancerProvider distancer.Provider
	// 对象池
	pools                  *pools

	// forbidFlat bool // mostly used in testing scenarios where we want to use the index even in scenarios where we typically wouldn't
	// 禁止平面搜索 - 主要用于测试场景，即使在通常不会使用索引的情况下也要使用索引
	forbidFlat bool

	// 性能指标收集器
	metrics       *Metrics
	// 插入操作指标
	insertMetrics *insertMetrics

	// randFunc func() float64 // added to temporarily get rid on flakiness in tombstones related tests. to be removed after fixing WEAVIATE-179
	// 随机函数 - 临时添加以解决墓碑相关测试中的不稳定问题
	// 修复 WEAVIATE-179 后应移除
	randFunc func() float64

	// The deleteVsInsertLock makes sure that there are no concurrent delete and
	// insert operations happening. It uses an RW-Mutex with:
	//
	// RLock -> Insert operations, this means any number of import operations can
	// happen concurrently.
	//
	// Lock -> Delete operation. This means only a single delete operation can
	// occur at a time, no insert operation can occur simultaneously with a
	// delete. Since the delete is cheap (just marking the node as deleted), the
	// single-threadedness of deletes is not a big problem.
	//
	// This lock was introduced as part of
	// https://github.com/weaviate/weaviate/issues/2194
	//
	// See
	// https://github.com/weaviate/weaviate/pull/2191#issuecomment-1242726787
	// where we ran performance tests to make sure introducing this lock has no
	// negative impact on performance.
	// 删除与插入操作互斥锁，确保不会同时发生删除和插入操作
	// 使用读写锁实现：
	// RLock -> 插入操作，允许多个导入操作并发执行
	// Lock -> 删除操作，同一时间只能有一个删除操作执行
	// 由于删除操作很轻量（只是标记节点为已删除），删除的单线程性不是大问题
	deleteVsInsertLock sync.RWMutex

	// 是否启用压缩
	compressed       atomic.Bool
	// 是否禁用重评分
	doNotRescore     bool
	// 是否启用 ACORN 搜索
	acornSearch      atomic.Bool
	// ACORN 过滤比例阈值
	acornFilterRatio float64

	// 是否禁用快照功能
	disableSnapshots  bool
	// 是否在启动时创建快照
	snapshotOnStartup bool

	// 向量压缩器
	compressor compressionhelpers.VectorCompressor
	// PQ（乘积量化）配置
	pqConfig   ent.PQConfig
	// BQ（二进制量化）配置
	bqConfig   ent.BQConfig
	// SQ（标量量化）配置
	sqConfig   ent.SQConfig
	// RQ（旋转量化）配置
	rqConfig   ent.RQConfig
	// RQ 是否激活
	rqActive   bool
	// rescoring compressed vectors is disk-bound. On cold starts, we cannot
	// rescore sequentially, as that would take very long. This setting allows us
	// to define the rescoring concurrency.
	// 重评分压缩向量受磁盘I/O限制。在冷启动时，我们不能顺序重评分，因为那样会花费很长时间
	// 此设置允许我们定义重评分的并发度
	rescoreConcurrency int

	// 压缩操作锁
	compressActionLock    *sync.RWMutex
	// 类名
	className             string
	// 分片名
	shardName             string
	// 根据ID获取向量的函数钩子
	VectorForIDThunk      common.VectorForID[float32]
	// 多向量获取函数钩子
	MultiVectorForIDThunk common.VectorForID[[]float32]
	// 分片节点锁
	shardedNodeLocks      *common.ShardedRWLocks
	// LSM-KV 存储
	store                 *lsmkv.Store

	// 内存分配检查器
	allocChecker            memwatch.AllocChecker
	// 墓碑清理是否正在运行
	tombstoneCleanupRunning atomic.Bool

	// 访问列表池最大大小
	visitedListPoolMaxSize int

	// 是否启用异步索引
	asyncIndexingEnabled bool

	// only used for multivector mode
	// 仅用于多向量模式
	multivector       atomic.Bool
	// Muvera 编码是否启用
	muvera            atomic.Bool
	// Muvera 编码器
	muveraEncoder     *multivector.MuveraEncoder
	// 文档ID到向量ID的映射
	docIDVectors      map[uint64][]uint64
	// 向量ID计数器
	vecIDcounter      uint64
	// 最大文档ID
	maxDocID          uint64
	// 创建存储桶的选项
	makeBucketOptions lsmkv.MakeBucketOptions

	// 文件系统接口
	fs common.FS
}

// Get 根据ID获取向量
// 如果索引未压缩，从缓存获取；如果已压缩，从压缩器获取
func (h *hnsw) Get(id uint64) ([]float32, error) {
	if !h.compressed.Load() {
		return h.cache.Get(context.Background(), id)
	}
	return h.compressor.Get(id)
}

// GetCompressedVector retrieves the compressed vector for a given ID.
// The index must be compressed, otherwise an error is returned.
// 获取指定ID的压缩向量
// 索引必须是压缩的，否则返回错误
func GetCompressedVector[T byte | uint64](h *hnsw, id uint64) ([]T, error) {
	if !h.compressed.Load() {
		return nil, errors.New("index is not compressed")
	}

	v, err := h.compressor.GetCompressed(id)
	if err != nil {
		return nil, err
	}

	return v.([]T), nil
}

// CommitLogger 提交日志接口，用于持久化HNSW索引操作
// 包含节点操作、链接管理、墓碑处理、压缩等功能
type CommitLogger interface {
	// 获取日志ID
	ID() string
	// 添加节点
	AddNode(node *vertex) error
	// 设置入口点及其最大层级
	SetEntryPointWithMaxLayer(id uint64, level int) error
	// 在指定层级添加链接
	AddLinkAtLevel(nodeid uint64, level int, target uint64) error
	// 替换指定层级的链接
	ReplaceLinksAtLevel(nodeid uint64, level int, targets []uint64) error
	// 添加墓碑标记
	AddTombstone(nodeid uint64) error
	// 移除墓碑标记
	RemoveTombstone(nodeid uint64) error
	// 删除节点
	DeleteNode(nodeid uint64) error
	// 清除节点的所有链接
	ClearLinks(nodeid uint64) error
	// 清除节点在指定层级的链接
	ClearLinksAtLevel(nodeid uint64, level uint16) error
	// 重置日志
	Reset() error
	// 删除日志文件
	Drop(ctx context.Context, keepFiles bool) error
	// 刷新缓冲区到磁盘
	Flush() error
	// 关闭日志
	Shutdown(ctx context.Context) error
	// 获取根路径
	RootPath() string
	// 为备份准备
	PrepareForBackup(bool) error
	// 添加PQ压缩数据
	AddPQCompression(compression.PQData) error
	// 添加SQ压缩数据
	AddSQCompression(compression.SQData) error
	// 添加Muvera数据
	AddMuvera(multivector.MuveraData) error
	// 添加RQ压缩数据
	AddRQCompression(compression.RQData) error
	// 添加BRQ压缩数据
	AddBRQCompression(compression.BRQData) error
	// 初始化维护模式
	InitMaintenance()

	// 创建快照
	CreateSnapshot() (bool, int64, error)
	// 创建并加载快照
	CreateAndLoadSnapshot() (*DeserializationResult, int64, error)
	// 加载快照
	LoadSnapshot() (*DeserializationResult, int64, error)
}

// BufferedLinksLogger 缓冲链接日志接口
// 用于批量处理链接操作，提高性能
type BufferedLinksLogger interface {
	// 在指定层级添加链接
	AddLinkAtLevel(nodeid uint64, level int, target uint64) error
	// 替换指定层级的链接
	ReplaceLinksAtLevel(nodeid uint64, level int, targets []uint64) error
	// 关闭日志（应先刷新再关闭）
	Close() error // Close should Flush and Close
}

// MakeCommitLogger 创建提交日志的函数类型
// 通过函数钩子的方式创建提交日志
type MakeCommitLogger func() (CommitLogger, error)

// HNSW 是 hnsw 结构体的别名
type HNSW = hnsw

// New creates a new HNSW index, the commit logger is provided through a thunk
// (a function which can be deferred). This is because creating a commit logger
// opens files for writing. However, checking whether a file is present, is a
// criterium for the index to see if it has to recover from disk or if its a
// truly new index. So instead the index is initialized, with un-biased disk
// checks first and only then is the commit logger created
// 创建新的HNSW索引，提交日志通过函数钩子提供
// （可以延迟执行的函数）。这是因为创建提交日志会打开文件进行写入
// 然而，检查文件是否存在是索引判断是否需要从磁盘恢复还是真正的新索引的标准
// 所以索引先进行无偏向的磁盘检查，然后才创建提交日志
func New(cfg Config, uc ent.UserConfig,
	tombstoneCallbacks cyclemanager.CycleCallbackGroup, store *lsmkv.Store,
) (*HNSW, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if cfg.Logger == nil {
		logger := logrus.New()
		logger.Out = io.Discard
		cfg.Logger = logger
	}

	normalizeOnRead := cfg.DistanceProvider.Type() == "cosine-dot"

	var vectorCache cache.Cache[float32]

	var muveraEncoder *multivector.MuveraEncoder
	if uc.Multivector.Enabled && !uc.Multivector.MuveraConfig.Enabled {
		vectorCache = cache.NewShardedMultiFloat32LockCache(cfg.MultiVectorForIDThunk, uc.VectorCacheMaxObjects,
			cfg.Logger, normalizeOnRead, cache.DefaultDeletionInterval, cfg.AllocChecker)
	} else {
		if uc.Multivector.MuveraConfig.Enabled {
			muveraEncoder = multivector.NewMuveraEncoder(uc.Multivector.MuveraConfig, store)
			err := store.CreateOrLoadBucket(
				context.Background(),
				cfg.ID+"_muvera_vectors",
				cfg.MakeBucketOptions(lsmkv.StrategyReplace)...,
			)
			if err != nil {
				return nil, errors.Wrapf(err, "Create or load bucket (muvera store)")
			}
			muveraVectorForID := func(ctx context.Context, id uint64) ([]float32, error) {
				return muveraEncoder.GetMuveraVectorForID(id, cfg.ID+"_muvera_vectors")
			}
			vectorCache = cache.NewShardedFloat32LockCache(
				muveraVectorForID, cfg.MultiVectorForIDThunk, uc.VectorCacheMaxObjects, 1, cfg.Logger,
				normalizeOnRead, cache.DefaultDeletionInterval, cfg.AllocChecker)

		} else {
			vectorCache = cache.NewShardedFloat32LockCache(cfg.VectorForIDThunk, cfg.MultiVectorForIDThunk, uc.VectorCacheMaxObjects, 1, cfg.Logger,
				normalizeOnRead, cache.DefaultDeletionInterval, cfg.AllocChecker)
		}
	}
	resetCtx, resetCtxCancel := context.WithCancel(context.Background())
	shutdownCtx, shutdownCtxCancel := context.WithCancel(context.Background())
	index := &hnsw{
		maximumConnections: uc.MaxConnections,

		// inspired by original paper and other implementations
		maximumConnectionsLayerZero: 2 * uc.MaxConnections,

		// inspired by c++ implementation
		levelNormalizer:       1 / math.Log(float64(uc.MaxConnections)),
		efConstruction:        uc.EFConstruction,
		flatSearchCutoff:      int64(uc.FlatSearchCutoff),
		flatSearchConcurrency: max(cfg.FlatSearchConcurrency, 1),
		acornFilterRatio:      cfg.AcornFilterRatio,
		disableSnapshots:      cfg.DisableSnapshots,
		snapshotOnStartup:     cfg.SnapshotOnStartup,
		nodes:                 make([]*vertex, cache.InitialSize),
		cache:                 vectorCache,
		waitForCachePrefill:   cfg.WaitForCachePrefill,
		cachePrefilled:        atomic.Bool{}, // Will be set appropriately in init()
		vectorForID:           vectorCache.Get,
		multiVectorForID:      vectorCache.MultiGet,
		id:                    cfg.ID,
		rootPath:              cfg.RootPath,
		tombstones:            map[uint64]struct{}{},
		logger:                cfg.Logger,
		distancerProvider:     cfg.DistanceProvider,
		deleteLock:            &sync.Mutex{},
		tombstoneLock:         &sync.RWMutex{},
		resetLock:             &sync.RWMutex{},
		resetCtx:              resetCtx,
		resetCtxCancel:        resetCtxCancel,
		shutdownCtx:           shutdownCtx,
		shutdownCtxCancel:     shutdownCtxCancel,
		initialInsertOnce:     &sync.Once{},

		ef:       int64(uc.EF),
		efMin:    int64(uc.DynamicEFMin),
		efMax:    int64(uc.DynamicEFMax),
		efFactor: int64(uc.DynamicEFFactor),

		metrics:   NewMetrics(cfg.PrometheusMetrics, cfg.ClassName, cfg.ShardName),
		shardName: cfg.ShardName,

		randFunc:                          rand.Float64,
		compressActionLock:                &sync.RWMutex{},
		className:                         cfg.ClassName,
		VectorForIDThunk:                  cfg.VectorForIDThunk,
		MultiVectorForIDThunk:             cfg.MultiVectorForIDThunk,
		TempMultiVectorForIDThunk:         cfg.TempMultiVectorForIDThunk,
		GetViewThunk:                      cfg.GetViewThunk,
		TempVectorForIDWithViewThunk:      cfg.TempVectorForIDWithViewThunk,
		TempMultiVectorForIDWithViewThunk: cfg.TempMultiVectorForIDWithViewThunk,
		pqConfig:                          uc.PQ,
		bqConfig:                          uc.BQ,
		sqConfig:                          uc.SQ,
		rqConfig:                          uc.RQ,
		rescoreConcurrency:                2 * runtime.GOMAXPROCS(0), // our default for IO-bound activties
		shardedNodeLocks:                  common.NewDefaultShardedRWLocks(),

		store:                  store,
		allocChecker:           cfg.AllocChecker,
		visitedListPoolMaxSize: cfg.VisitedListPoolMaxSize,
		asyncIndexingEnabled:   cfg.AsyncIndexingEnabled,

		docIDVectors:      make(map[uint64][]uint64),
		muveraEncoder:     muveraEncoder,
		makeBucketOptions: cfg.MakeBucketOptions,
		fs:                common.NewOSFS(),
	}
	index.logger = cfg.Logger.WithFields(logrus.Fields{
		"shard":        cfg.ShardName,
		"class":        cfg.ClassName,
		"targetVector": index.getTargetVector(),
	})
	index.acornSearch.Store(uc.FilterStrategy == ent.FilterStrategyAcorn)

	index.multivector.Store(uc.Multivector.Enabled)
	index.muvera.Store(uc.Multivector.MuveraConfig.Enabled)

	if uc.BQ.Enabled {
		var err error
		if uc.Multivector.Enabled && !uc.Multivector.MuveraConfig.Enabled {
			index.compressor, err = compressionhelpers.NewBQMultiCompressor(
				index.distancerProvider, uc.VectorCacheMaxObjects, cfg.Logger, store,
				cfg.MakeBucketOptions, cfg.AllocChecker, index.getTargetVector())
		} else {
			index.compressor, err = compressionhelpers.NewBQCompressor(
				index.distancerProvider, uc.VectorCacheMaxObjects, cfg.Logger, store,
				cfg.MakeBucketOptions, cfg.AllocChecker, index.getTargetVector())
		}
		if err != nil {
			return nil, err
		}
		index.compressed.Store(true)
		index.cache.Drop()
		index.cache = nil
	}

	if uc.RQ.Enabled {
		index.rqActive = true
	}

	if uc.Multivector.Enabled {
		index.multiDistancerProvider = distancer.NewDotProductProvider()
		if !uc.Multivector.MuveraConfig.Enabled {
			err := index.store.CreateOrLoadBucket(
				context.Background(),
				cfg.ID+"_mv_mappings",
				cfg.MakeBucketOptions(lsmkv.StrategyReplace)...,
			)
			if err != nil {
				return nil, errors.Wrapf(err, "Create or load bucket (multivector store)")
			}
		}
	}

	if err := index.init(cfg); err != nil {
		return nil, errors.Wrapf(err, "init index %q", index.id)
	}

	// TODO common_cycle_manager move to poststartup?
	id := strings.Join([]string{
		"hnsw", "tombstone_cleanup",
		index.className, index.shardName, index.id,
	}, "/")
	index.tombstoneCleanupCallbackCtrl = tombstoneCallbacks.Register(id, index.tombstoneCleanup)
	index.insertMetrics = newInsertMetrics(index.metrics)

	return index, nil
}

func (h *hnsw) getTargetVector() string {
	if name, found := strings.CutPrefix(h.id, fmt.Sprintf("%s_", helpers.VectorsBucketLSM)); found {
		return name
	}
	// legacy vector index
	return ""
}

// TODO: use this for incoming replication
// func (h *hnsw) insertFromExternal(nodeId, targetLevel int, neighborsAtLevel map[int][]uint32) {
// 	defer m.addBuildingReplication(time.Now())

// 	// randomly introduce up to 50ms delay to account for network slowness
// 	time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

// 	var node *hnswVertex
// 	h.RLock()
// 	total := len(h.nodes)
// 	if total > nodeId {
// 		node = h.nodes[nodeId] // it could be that we implicitly added this node already because it was referenced
// 	}
// 	h.RUnlock()

// 	if node == nil {
// 		node = &hnswVertex{
// 			id:          nodeId,
// 			connections: make(map[int][]uint32),
// 			level:       targetLevel,
// 		}
// 	} else {
// 		node.level = targetLevel
// 	}

// 	if total == 0 {
// 		h.Lock()
// 		h.commitLog.SetEntryPointWithMaxLayer(node.id, 0)
// 		h.entryPointID = node.id
// 		h.currentMaximumLayer = 0
// 		node.connections = map[int][]uint32{}
// 		node.level = 0
// 		// h.nodes = make([]*hnswVertex, 100000)
// 		h.commitLog.AddNode(node)
// 		h.nodes[node.id] = node
// 		h.Unlock()
// 		return
// 	}

// 	currentMaximumLayer := h.currentMaximumLayer
// 	h.Lock()
// 	h.nodes[nodeId] = node
// 	h.commitLog.AddNode(node)
// 	h.Unlock()

// 	for level := min(targetLevel, currentMaximumLayer); level >= 0; level-- {
// 		neighbors := neighborsAtLevel[level]

// 		for _, neighborID := range neighbors {
// 			h.RLock()
// 			neighbor := h.nodes[neighborID]
// 			if neighbor == nil {
// 				// due to everything being parallel it could be that the linked neighbor
// 				// doesn't exist yet
// 				h.nodes[neighborID] = &hnswVertex{
// 					id:          int(neighborID),
// 					connections: make(map[int][]uint32),
// 				}
// 				neighbor = h.nodes[neighborID]
// 			}
// 			h.RUnlock()

// 			neighbor.linkAtLevel(level, uint32(nodeId), h.commitLog)
// 			node.linkAtLevel(level, uint32(neighbor.id), h.commitLog)

// 			neighbor.RLock()
// 			currentConnections := neighbor.connections[level]
// 			neighbor.RUnlock()

// 			maximumConnections := h.maximumConnections
// 			if level == 0 {
// 				maximumConnections = h.maximumConnectionsLayerZero
// 			}

// 			if len(currentConnections) <= maximumConnections {
// 				// nothing to do, skip
// 				continue
// 			}

// 			// TODO: support both neighbor selection algos
// 			updatedConnections := h.selectNeighborsSimpleFromId(nodeId, currentConnections, maximumConnections)

// 			neighbor.Lock()
// 			h.commitLog.ReplaceLinksAtLevel(neighbor.id, level, updatedConnections)
// 			neighbor.connections[level] = updatedConnections
// 			neighbor.Unlock()
// 		}
// 	}

// 	if targetLevel > h.currentMaximumLayer {
// 		h.Lock()
// 		h.commitLog.SetEntryPointWithMaxLayer(nodeId, targetLevel)
// 		h.entryPointID = nodeId
// 		h.currentMaximumLayer = targetLevel
// 		h.Unlock()
// 	}

// }

// findBestEntrypointForNode 为节点寻找最佳入口点
// 当新目标层级低于当前最大层级时，需要在每一层搜索更好的候选点并更新候选点
func (h *hnsw) findBestEntrypointForNode(ctx context.Context, currentMaxLevel, targetLevel int,
	entryPointID uint64, nodeVec []float32, distancer compressionhelpers.CompressorDistancer,
) (uint64, error) {
	// in case the new target is lower than the current max, we need to search
	// each layer for a better candidate and update the candidate
	// 如果新目标层级低于当前最大层级，我们需要在每一层搜索更好的候选点并更新候选点
	for level := currentMaxLevel; level > targetLevel; level-- {
		eps := priorityqueue.NewMin[any](1)
		var dist float32
		var err error
		if h.compressed.Load() {
			dist, err = distancer.DistanceToNode(entryPointID)
		} else {
			dist, err = h.distToNode(distancer, entryPointID, nodeVec)
		}

		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID, "findBestEntrypointForNode")
				continue
			}
			return 0, errors.Wrapf(err,
				"calculate distance between insert node and entry point at level %d", level)
		}

		eps.Insert(entryPointID, dist)
		res, err := h.searchLayerByVectorWithDistancer(ctx, nodeVec, eps, 1, level, nil, distancer)
		if err != nil {
			return 0,
				errors.Wrapf(err, "update candidate: search layer at level %d", level)
		}
		if res.Len() > 0 {
			// if we could find a new entrypoint, use it
			// in case everything was tombstoned, stick with the existing one
			// 如果能找到新的入口点就使用它
			// 如果所有节点都被墓碑标记了，就坚持使用现有的入口点
			elem := res.Pop()
			n := h.nodeByID(elem.ID)
			if n != nil && !n.isUnderMaintenance() {
				// but not if the entrypoint is under maintenance
				// 但如果入口点正在维护中就不使用
				entryPointID = elem.ID
			}
		}

		h.pools.pqResults.Put(res)
	}

	return entryPointID, nil
}

// distBetweenNodes 计算两个节点之间的距离
// 如果索引已压缩，使用压缩距离计算；否则使用原始向量计算
func (h *hnsw) distBetweenNodes(a, b uint64) (float32, error) {
	if h.compressed.Load() {
		dist, err := h.compressor.DistanceBetweenCompressedVectorsFromIDs(context.Background(), a, b)
		if err != nil {
			return 0, err
		}

		return dist, nil
	}

	// TODO: introduce single search/transaction context instead of spawning new
	// ones
	// TODO: 引入单一的搜索/事务上下文而不是创建新的
	vecA, errA := h.vectorForID(context.Background(), a)

	if errA != nil {
		var e storobj.ErrNotFound
		if errors.As(errA, &e) {
			h.handleDeletedNode(e.DocID, "distBetweenNodes")
			return 0, nil
		}
		// not a typed error, we can recover from, return with err
		// 不是类型化的错误，我们可以从中恢复，返回错误
		return 0, errors.Wrapf(errA,
			"could not get vector of object at docID %d", a)
	}

	if len(vecA) == 0 {
		return 0, fmt.Errorf("got a nil or zero-length vector at docID %d", a)
	}

	vecB, errB := h.vectorForID(context.Background(), b)

	if errB != nil {
		var e storobj.ErrNotFound
		if errors.As(errB, &e) {
			h.handleDeletedNode(e.DocID, "distBetweenNodes")
			return 0, nil
		}
		// not a typed error, we can recover from, return with err
		// 不是类型化的错误，我们可以从中恢复，返回错误
		return 0, errors.Wrapf(errB,
			"could not get vector of object at docID %d", b)
	}

	if len(vecB) == 0 {
		return 0, fmt.Errorf("got a nil or zero-length vector at docID %d", b)
	}

	return h.distancerProvider.SingleDist(vecA, vecB)
}

// distToNode 计算节点到向量的距离
// 如果索引已压缩，使用压缩距离计算器；否则计算原始距离
func (h *hnsw) distToNode(distancer compressionhelpers.CompressorDistancer, node uint64, vecB []float32) (float32, error) {
	if h.compressed.Load() {
		dist, err := distancer.DistanceToNode(node)
		if err != nil {
			return 0, err
		}

		return dist, nil
	}

	// TODO: introduce single search/transaction context instead of spawning new
	// ones
	var vecA []float32
	var err error
	vecA, err = h.vectorForID(context.Background(), node)
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID, "distBetweenNodeAndVec")
			return 0, nil
		}
		// not a typed error, we can recover from, return with err
		return 0, errors.Wrapf(err,
			"could not get vector of object at docID %d", node)
	}

	if len(vecA) == 0 {
		return 0, fmt.Errorf(
			"got a nil or zero-length vector at docID %d", node)
	}

	if len(vecB) == 0 {
		return 0, fmt.Errorf(
			"got a nil or zero-length vector as search vector")
	}

	return h.distancerProvider.SingleDist(vecA, vecB)
}

func (h *hnsw) isEmpty() bool {
	h.RLock()
	defer h.RUnlock()
	h.shardedNodeLocks.RLock(h.entryPointID)
	defer h.shardedNodeLocks.RUnlock(h.entryPointID)

	return h.isEmptyUnlocked()
}

func (h *hnsw) isEmptyUnlocked() bool {
	return h.entryPointID > uint64(len(h.nodes)) || h.nodes[h.entryPointID] == nil
}

func (h *hnsw) nodeByID(id uint64) *vertex {
	h.RLock()
	defer h.RUnlock()

	if id >= uint64(len(h.nodes)) {
		// See https://github.com/weaviate/weaviate/issues/1838 for details.
		// This could be after a crash recovery when the object store is "further
		// ahead" than the hnsw index and we receive a delete request
		return nil
	}

	h.shardedNodeLocks.RLock(id)
	defer h.shardedNodeLocks.RUnlock(id)

	return h.nodes[id]
}

func (h *hnsw) Drop(ctx context.Context, keepFiles bool) error {
	// cancel tombstone cleanup goroutine
	if err := h.tombstoneCleanupCallbackCtrl.Unregister(ctx); err != nil {
		return errors.Wrap(err, "hnsw drop")
	}

	if h.compressed.Load() {
		err := h.compressor.Drop()
		if err != nil {
			return fmt.Errorf("failed to shutdown compressed store")
		}
	} else {
		// cancel vector cache goroutine
		h.cache.Drop()
	}

	// cancel commit logger last, as the tombstone cleanup cycle might still
	// write while it's still running
	err := h.commitLog.Drop(ctx, keepFiles)
	if err != nil {
		return errors.Wrap(err, "commit log drop")
	}

	return nil
}

func (h *hnsw) Shutdown(ctx context.Context) error {
	h.shutdownCtxCancel()

	if err := h.commitLog.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "hnsw shutdown")
	}

	if err := h.tombstoneCleanupCallbackCtrl.Unregister(ctx); err != nil {
		return errors.Wrap(err, "hnsw shutdown")
	}

	if h.compressed.Load() {
		err := h.compressor.Drop()
		if err != nil {
			return errors.Wrap(err, "hnsw shutdown")
		}
	} else {
		h.cache.Drop()
	}

	return nil
}

// Flush 将提交日志缓冲区刷新到磁盘
func (h *hnsw) Flush() error {
	return h.commitLog.Flush()
}

// Entrypoint 获取当前入口点ID
func (h *hnsw) Entrypoint() uint64 {
	h.RLock()
	defer h.RUnlock()

	return h.entryPointID
}

// ContainsDoc 检查文档ID是否存在于索引中
// 对于多向量模式和普通模式采用不同的检查逻辑
func (h *hnsw) ContainsDoc(docID uint64) bool {
	if h.Multivector() && !h.muvera.Load() {
		h.RLock()
		vecIds, exists := h.docIDVectors[docID]
		h.RUnlock()
		return exists && !h.hasTombstones(vecIds)
	}

	h.RLock()
	h.shardedNodeLocks.RLock(docID)
	exists := len(h.nodes) > int(docID) && h.nodes[docID] != nil
	h.shardedNodeLocks.RUnlock(docID)
	h.RUnlock()

	return exists && !h.hasTombstone(docID)
}

// Iterate 遍历索引中的所有文档ID
// 根据是否为多向量模式选择不同的遍历方法
func (h *hnsw) Iterate(fn func(docID uint64) bool) {
	if h.Multivector() && !h.muvera.Load() {
		h.iterateMulti(fn)
		return
	}
	h.iterate(fn)
}

// iterate 遍历普通模式下的所有节点
// 检查关闭和重置状态，跳过墓碑节点
func (h *hnsw) iterate(fn func(docID uint64) bool) {
	var id uint64

	for {
		if h.shutdownCtx.Err() != nil {
			return
		}
		if h.resetCtx.Err() != nil {
			return
		}

		h.RLock()
		h.shardedNodeLocks.RLock(id)
		stop := int(id) >= len(h.nodes)
		exists := !stop && h.nodes[id] != nil
		h.shardedNodeLocks.RUnlock(id)
		h.RUnlock()

		if stop {
			return
		}

		if exists && !h.hasTombstone(id) {
			if !fn(id) {
				return
			}
		}

		id++
	}
}

// iterateMulti 遍历多向量模式下的所有文档
// 先收集所有文档ID，然后逐一检查
func (h *hnsw) iterateMulti(fn func(docID uint64) bool) {
	h.RLock()
	indexedDocIDs := make([]uint64, 0, len(h.docIDVectors))
	for docID := range h.docIDVectors {
		indexedDocIDs = append(indexedDocIDs, docID)
	}
	h.RUnlock()

	for _, docID := range indexedDocIDs {
		if h.shutdownCtx.Err() != nil || h.resetCtx.Err() != nil {
			return
		}

		h.RLock()
		nodes, ok := h.docIDVectors[docID]
		h.RUnlock()

		if ok && !h.hasTombstones(nodes) {
			if !fn(docID) {
				return
			}
		}
	}
}

func (h *hnsw) ShouldUpgrade() (bool, int) {
	h.compressActionLock.RLock()
	defer h.compressActionLock.RUnlock()
	// Don't upgrade if cache is not prefilled yet
	if !h.cachePrefilled.Load() {
		return false, 0
	}
	if h.sqConfig.Enabled {
		return h.sqConfig.Enabled, h.sqConfig.TrainingLimit
	}
	if h.rqConfig.Enabled {
		return h.rqConfig.Enabled, 1
	}
	return h.pqConfig.Enabled, h.pqConfig.TrainingLimit
}

func (h *hnsw) ShouldCompressFromConfig(config config.VectorIndexConfig) (bool, int) {
	hnswConfig := config.(ent.UserConfig)
	if hnswConfig.SQ.Enabled {
		return hnswConfig.SQ.Enabled, hnswConfig.SQ.TrainingLimit
	}
	if hnswConfig.RQ.Enabled {
		return hnswConfig.RQ.Enabled, 1
	}
	return hnswConfig.PQ.Enabled, hnswConfig.PQ.TrainingLimit
}

func (h *hnsw) Compressed() bool {
	return h.compressed.Load()
}

func (h *hnsw) Multivector() bool {
	return h.multivector.Load()
}

func (h *hnsw) Upgraded() bool {
	return h.Compressed()
}

func (h *hnsw) AlreadyIndexed() uint64 {
	if h.compressed.Load() {
		return uint64(h.compressor.CountVectors())
	}
	return uint64(h.cache.CountVectors())
}

func (h *hnsw) CurrentVectorsLen() uint64 {
	if h.compressed.Load() {
		return h.compressor.MaxVectorID()
	}
	return uint64(h.cache.Len())
}

// normalizeVec 对向量进行归一化处理
// cosine-dot 距离需要归一化向量，因为点积和余弦相似度只有在向量归一化时才相等
func (h *hnsw) normalizeVec(vec []float32) []float32 {
	if h.distancerProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		// cosine-dot 需要归一化向量，因为点积和余弦相似度只有在向量归一化时才相等
		return distancer.Normalize(vec)
	}
	return vec
}

// normalizeVecInPlace normalizes the vector in-place without allocating.
// Use this only when the caller owns the vector and doesn't need to preserve
// the original (e.g., pooled temporary vectors).
// 就地归一化向量而不分配新内存
// 仅当调用者拥有向量且不需要保留原始向量时使用（例如池化的临时向量）
func (h *hnsw) normalizeVecInPlace(vec []float32) {
	if h.distancerProvider.Type() == "cosine-dot" {
		distancer.NormalizeInPlace(vec)
	}
}

// normalizeVecs 对多个向量进行归一化处理
func (h *hnsw) normalizeVecs(vecs [][]float32) [][]float32 {
	if h.distancerProvider.Type() == "cosine-dot" {
		normalized := make([][]float32, len(vecs))
		for i, vec := range vecs {
			normalized[i] = distancer.Normalize(vec)
		}
		return normalized
	}
	return vecs
}

// IsHNSWIndex 判断给定索引是否为HNSW索引
func IsHNSWIndex(index any) bool {
	_, ok := index.(*hnsw)
	return ok
}

// AsHNSWIndex 将索引转换为HNSW索引
func AsHNSWIndex(index any) Index {
	h, _ := index.(*hnsw)
	return h
}

// This interface exposes public methods of the HNSW index
// that are not part of the VectorIndex interface.
// It is a workaround to avoid circular dependencies.
// 该接口暴露了HNSW索引的公共方法
// 这些方法不属于VectorIndex接口的一部分
// 这是一种避免循环依赖的变通方法
type Index interface {
	// 清理墓碑节点
	CleanUpTombstonedNodes(shouldAbort cyclemanager.ShouldAbortCallback) error
}

// nodeLevel 表示节点及其层级的结构体
type nodeLevel struct {
	// 节点ID
	nodeId uint64
	// 节点层级
	level  int
}

// calculateUnreachablePoints 计算不可达的节点点
// 通过图遍历找出无法从入口点到达的节点
func (h *hnsw) calculateUnreachablePoints() []uint64 {
	h.RLock()
	defer h.RUnlock()

	visitedPairs := make(map[nodeLevel]bool)
	candidateList := []nodeLevel{{h.entryPointID, h.currentMaximumLayer}}

	for len(candidateList) > 0 {
		currentNode := candidateList[len(candidateList)-1]
		candidateList = candidateList[:len(candidateList)-1]
		if !visitedPairs[currentNode] {
			visitedPairs[currentNode] = true
			h.shardedNodeLocks.RLock(currentNode.nodeId)
			node := h.nodes[currentNode.nodeId]
			if node != nil {
				node.Lock()
				neighbors := node.connectionsAtLowerLevelsNoLock(currentNode.level, visitedPairs)
				node.Unlock()
				candidateList = append(candidateList, neighbors...)
			}
			h.shardedNodeLocks.RUnlock(currentNode.nodeId)
		}
	}

	visitedNodes := make(map[uint64]bool, len(visitedPairs))
	for k, v := range visitedPairs {
		if v {
			visitedNodes[k.nodeId] = true
		}
	}

	unvisitedNodes := []uint64{}
	for i := 0; i < len(h.nodes); i++ {
		var id uint64
		h.shardedNodeLocks.RLock(uint64(i))
		if h.nodes[i] != nil {
			id = h.nodes[i].id
		}
		h.shardedNodeLocks.RUnlock(uint64(i))
		if id == 0 {
			continue
		}
		if !visitedNodes[uint64(i)] {
			unvisitedNodes = append(unvisitedNodes, id)
		}

	}
	return unvisitedNodes
}

type HnswStats struct {
	Dimensions         int32                               `json:"dimensions"`
	EntryPointID       uint64                              `json:"entryPointID"`
	DistributionLayers map[int]uint                        `json:"distributionLayers"`
	UnreachablePoints  []uint64                            `json:"unreachablePoints"`
	NumTombstones      int                                 `json:"numTombstones"`
	CacheSize          int32                               `json:"cacheSize"`
	Compressed         bool                                `json:"compressed"`
	CompressorStats    compressionhelpers.CompressionStats `json:"compressionStats"`
	CompressionType    string                              `json:"compressionType"`
}

func (s *HnswStats) IndexType() common.IndexType {
	return common.IndexTypeHNSW
}

// Stats 获取HNSW索引的统计信息
// 包括维度、入口点、层级分布、不可达点、墓碑数量等
func (h *hnsw) Stats() (*HnswStats, error) {
	h.RLock()
	defer h.RUnlock()
	distributionLayers := map[int]uint{}

	for _, node := range h.nodes {
		func() {
			if node == nil {
				return
			}
			node.Lock()
			defer node.Unlock()
			l := node.level
			if l == 0 && node.connections.Layers() == 0 {
				return
			}
			c, ok := distributionLayers[l]
			if !ok {
				distributionLayers[l] = 0
			}

			distributionLayers[l] = c + 1
		}()
	}

	stats := HnswStats{
		Dimensions:         h.dims,
		EntryPointID:       h.entryPointID,
		DistributionLayers: distributionLayers,
		UnreachablePoints:  h.calculateUnreachablePoints(),
		NumTombstones:      len(h.tombstones),
		Compressed:         h.compressed.Load(),
	}

	if stats.Compressed {
		stats.CompressorStats = h.compressor.Stats()
		stats.CacheSize = h.compressor.Len()
	} else {
		stats.CompressorStats = compressionhelpers.UncompressedStats{}
		stats.CacheSize = h.cache.Len()
	}

	stats.CompressionType = stats.CompressorStats.CompressionType()

	return &stats, nil
}

// Type 返回索引类型
func (h *hnsw) Type() common.IndexType {
	return common.IndexTypeHNSW
}

// CompressionStats 获取压缩统计信息
// 如果索引已压缩，返回压缩器统计；否则返回未压缩统计
func (h *hnsw) CompressionStats() compressionhelpers.CompressionStats {
	if h.compressed.Load() {
		return h.compressor.Stats()
	}
	return compressionhelpers.UncompressedStats{}
}
