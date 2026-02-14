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

package clients

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
)

// 异步复制目标节点端点模式常量
const (
	asyncReplicationTargetNodeEndpointPattern = "/indices/%s/shards/%s/async-replication-target-node"
)

// AsyncReplicationTargetNodeEndpoint 根据索引名和分片名生成异步复制目标节点的端点URL
func AsyncReplicationTargetNodeEndpoint(indexName, shardName string) string {
	return fmt.Sprintf(asyncReplicationTargetNodeEndpointPattern, indexName, shardName)
}

// RemoteIndex 远程索引客户端结构体，用于与远程Weaviate节点进行通信
type RemoteIndex struct {
	retryClient // 嵌入重试客户端，提供HTTP请求重试功能
}

// NewRemoteIndex 创建新的远程索引客户端实例
// 参数:
//   - httpClient: HTTP客户端实例
// 返回值: RemoteIndex指针
func NewRemoteIndex(httpClient *http.Client) *RemoteIndex {
	return &RemoteIndex{retryClient: retryClient{
		client:  httpClient, // 底层HTTP客户端
		retryer: newRetryer(), // 重试策略
	}}
}

// PutObject 在远程节点上存储单个对象
// 参数:
//   - ctx: 上下文对象
//   - host: 目标主机地址
//   - index: 索引名称
//   - shard: 分片名称
//   - obj: 要存储的对象
//   - schemaVersion: 模式版本号
// 返回值: 错误信息
func (c *RemoteIndex) PutObject(ctx context.Context, host, index,
	shard string, obj *storobj.Object, schemaVersion uint64,
) error {
	// 将模式版本号转换为字符串切片
	value := []string{strconv.FormatUint(schemaVersion, 10)}

	// 序列化对象数据
	body, err := clusterapi.IndicesPayloads.SingleObject.Marshal(obj, clusterapi.MethodPut)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	// 构建HTTP请求
	req, err := setupRequest(ctx, http.MethodPost, host,
		fmt.Sprintf("/indices/%s/shards/%s/objects", index, shard),
		url.Values{replica.SchemaVersionKey: value}.Encode(), // 添加模式版本查询参数
		bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}

	// 设置内容类型头部
	clusterapi.IndicesPayloads.SingleObject.SetContentTypeHeaderReq(req)
	// 执行请求并等待响应
	_, err = c.do(c.timeoutUnit*COMMIT_TIMEOUT_VALUE, req, body, nil, successCode)
	return err
}

// duplicateErr 将单个错误复制到指定长度的错误切片中
// 参数:
//   - in: 原始错误
//   - count: 需要复制的数量
// 返回值: 包含相同错误的错误切片
func duplicateErr(in error, count int) []error {
	out := make([]error, count)
	for i := range out {
		out[i] = in
	}
	return out
}

// BatchPutObjects 批量存储对象到远程节点
// 参数:
//   - ctx: 上下文对象
//   - host: 目标主机地址
//   - index: 索引名称
//   - shard: 分片名称
//   - objs: 对象数组
//   - _ : 复制属性(未使用)
//   - schemaVersion: 模式版本号
// 返回值: 错误数组，每个对象对应一个错误
func (c *RemoteIndex) BatchPutObjects(ctx context.Context, host, index,
	shard string, objs []*storobj.Object, _ *additional.ReplicationProperties, schemaVersion uint64,
) []error {
	// 将模式版本号转换为字符串切片
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	// 序列化对象列表
	body, err := clusterapi.IndicesPayloads.ObjectList.Marshal(objs, clusterapi.MethodPut)
	if err != nil {
		return duplicateErr(fmt.Errorf("encode request: %w", err), len(objs))
	}

	// 构建HTTP请求
	req, err := setupRequest(ctx, http.MethodPost, host,
		fmt.Sprintf("/indices/%s/shards/%s/objects", index, shard),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
		bytes.NewReader(body))
	if err != nil {
		return duplicateErr(fmt.Errorf("create http request: %w", err), len(objs))
	}
	// 设置内容类型头部
	clusterapi.IndicesPayloads.ObjectList.SetContentTypeHeaderReq(req)

	var resp []error
	// 定义自定义解码函数来处理响应
	decode := func(data []byte) error {
		resp = clusterapi.IndicesPayloads.ErrorList.Unmarshal(data)
		return nil
	}

	// 使用自定义marshaller执行请求
	if err = c.doWithCustomMarshaller(c.timeoutUnit*COMMIT_TIMEOUT_VALUE, req, body, decode, successCode, MAX_RETRIES); err != nil {
		return duplicateErr(err, len(objs))
	}

	return resp
}

// BatchAddReferences 批量添加引用关系
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - refs: 引用关系批处理数据
//   - schemaVersion: 模式版本号
// 返回值: 错误数组
func (c *RemoteIndex) BatchAddReferences(ctx context.Context, hostName, indexName,
	shardName string, refs objects.BatchReferences, schemaVersion uint64,
) []error {
	// 将模式版本号转换为字符串切片
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	// 序列化引用列表
	marshalled, err := clusterapi.IndicesPayloads.ReferenceList.Marshal(refs)
	if err != nil {
		return duplicateErr(errors.Wrap(err, "marshal payload"), len(refs))
	}

	// 构建HTTP请求
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/references", indexName, shardName),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
		bytes.NewReader(marshalled))
	if err != nil {
		return duplicateErr(errors.Wrap(err, "open http request"), len(refs))
	}

	// 设置内容类型头部
	clusterapi.IndicesPayloads.ReferenceList.SetContentTypeHeaderReq(req)

	// 发送HTTP请求
	res, err := c.client.Do(req)
	if err != nil {
		return duplicateErr(errors.Wrap(err, "send http request"), len(refs))
	}

	defer res.Body.Close()
	// 检查响应状态码
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return duplicateErr(errors.Errorf("unexpected status code %d (%s)",
			res.StatusCode, body), len(refs))
	}

	// 验证内容类型
	if ct, ok := clusterapi.IndicesPayloads.ErrorList.
		CheckContentTypeHeader(res); !ok {
		return duplicateErr(errors.Errorf("unexpected content type: %s",
			ct), len(refs))
	}

	// 读取响应体
	resBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return duplicateErr(errors.Wrap(err, "ready body"), len(refs))
	}

	// 反序列化错误列表并返回
	return clusterapi.IndicesPayloads.ErrorList.Unmarshal(resBytes)
}

// GetObject 从远程节点获取指定ID的对象
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - id: 对象UUID
//   - selectProps: 选择属性
//   - additional: 附加属性
// 返回值: 对象指针和错误信息
func (c *RemoteIndex) GetObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID, selectProps search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	// 序列化选择属性
	selectPropsBytes, err := json.Marshal(selectProps)
	if err != nil {
		return nil, errors.Wrap(err, "marshal selectProps props")
	}

	// 序列化附加属性
	additionalBytes, err := json.Marshal(additional)
	if err != nil {
		return nil, errors.Wrap(err, "marshal additional props")
	}

	// 对属性进行Base64编码
	selectPropsEncoded := base64.StdEncoding.EncodeToString(selectPropsBytes)
	additionalEncoded := base64.StdEncoding.EncodeToString(additionalBytes)

	// 构建GET请求
	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects/%s", indexName, shardName, id),
		url.Values{
			"additional":       []string{additionalEncoded},
			"selectProperties": []string{selectPropsEncoded},
		}.Encode(),
		nil)
	if err != nil {
		return nil, errors.Wrap(err, "open http request")
	}

	// 发送请求
	res, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	// 处理对象不存在的情况
	if res.StatusCode == http.StatusNotFound {
		// 这是合法情况 - 请求的ID不存在，不需要反序列化任何内容
		return nil, nil
	}

	// 检查成功状态码
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return nil, errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	// 验证内容类型
	ct, ok := clusterapi.IndicesPayloads.SingleObject.CheckContentTypeHeader(res)
	if !ok {
		return nil, errors.Errorf("unknown content type %s", ct)
	}

	// 读取响应体
	objBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read body")
	}

	// 反序列化对象
	obj, err := clusterapi.IndicesPayloads.SingleObject.Unmarshal(objBytes, clusterapi.MethodGet)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal body")
	}

	return obj, nil
}

// Exists 检查指定ID的对象是否存在于远程节点上
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - id: 对象UUID
// 返回值: 存在状态布尔值和错误信息
func (c *RemoteIndex) Exists(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID,
) (bool, error) {
	// 构建检查存在性的GET请求
	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects/%s", indexName, shardName, id),
		url.Values{"check_exists": []string{"true"}}.Encode(), // 添加检查存在性参数
		nil)
	if err != nil {
		return false, fmt.Errorf("create http request: %w", err)
	}
	// 定义成功的状态码条件
	ok := func(code int) bool { return code == http.StatusNotFound || code == http.StatusNoContent }
	// 执行请求
	code, err := c.do(c.timeoutUnit*QUERY_TIMEOUT_VALUE, req, nil, nil, ok)
	// 如果返回状态不是NotFound，则对象存在
	return code != http.StatusNotFound, err
}

// DeleteObject 从远程节点删除指定ID的对象
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - id: 对象UUID
//   - deletionTime: 删除时间戳
//   - schemaVersion: 模式版本号
// 返回值: 错误信息
func (c *RemoteIndex) DeleteObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) error {
	// 将模式版本号转换为字符串切片
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	// 构建DELETE请求，包含删除时间和模式版本
	req, err := setupRequest(ctx, http.MethodDelete, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects/%s/%d", indexName, shardName, id, deletionTime.UnixMilli()),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
		nil)
	if err != nil {
		return errors.Wrap(err, "open http request")
	}

	// 发送删除请求
	res, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	// 处理对象已不存在的情况
	if res.StatusCode == http.StatusNotFound {
		// 这是合法情况 - 请求的ID不存在，不需要反序列化任何内容，可以假设已被删除
		return nil
	}

	// 检查删除成功状态
	if res.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(res.Body)
		return errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	return nil
}

// MergeObject 合并更新远程节点上的对象
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - mergeDoc: 合并文档
//   - schemaVersion: 模式版本号
// 返回值: 错误信息
func (c *RemoteIndex) MergeObject(ctx context.Context, hostName, indexName,
	shardName string, mergeDoc objects.MergeDocument, schemaVersion uint64,
) error {
	// 将模式版本号转换为字符串切片
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	// 序列化合并文档
	marshalled, err := clusterapi.IndicesPayloads.MergeDoc.Marshal(mergeDoc)
	if err != nil {
		return errors.Wrap(err, "marshal payload")
	}
	// 构建PATCH请求
	req, err := setupRequest(ctx, http.MethodPatch, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects/%s", indexName, shardName,
			mergeDoc.ID),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
		bytes.NewReader(marshalled))
	if err != nil {
		return errors.Wrap(err, "open http request")
	}

	// 设置内容类型头部
	clusterapi.IndicesPayloads.MergeDoc.SetContentTypeHeaderReq(req)
	// 发送合并请求
	res, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	// 检查合并成功状态
	if res.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(res.Body)
		return errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	return nil
}

// MultiGetObjects 批量获取多个对象
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - ids: 对象UUID数组
// 返回值: 对象数组和错误信息
func (c *RemoteIndex) MultiGetObjects(ctx context.Context, hostName, indexName,
	shardName string, ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	// 序列化ID数组
	idsBytes, err := json.Marshal(ids)
	if err != nil {
		return nil, errors.Wrap(err, "marshal selectProps props")
	}
	// 对ID进行Base64编码
	idsEncoded := base64.StdEncoding.EncodeToString(idsBytes)
	// 构建批量获取请求
	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects", indexName, shardName),
		url.Values{"ids": []string{idsEncoded}}.Encode(),
		nil)
	if err != nil {
		return nil, errors.Wrap(err, "open http request")
	}

	// 发送请求
	res, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	// 处理对象不存在的情况
	if res.StatusCode == http.StatusNotFound {
		// 这是合法情况 - 请求的ID不存在，不需要反序列化任何内容
		return nil, nil
	}

	// 检查成功状态码
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return nil, errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	// 验证内容类型
	ct, ok := clusterapi.IndicesPayloads.ObjectList.CheckContentTypeHeader(res)
	if !ok {
		return nil, errors.Errorf("unexpected content type: %s", ct)
	}

	// 读取响应体
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	// 反序列化对象列表
	objs, err := clusterapi.IndicesPayloads.ObjectList.Unmarshal(bodyBytes, clusterapi.MethodGet)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal objects")
	}

	return objs, nil
}

// SearchShard 在指定分片上执行搜索操作
// 参数:
//   - ctx: 上下文对象
//   - host: 目标主机
//   - index: 索引名称
//   - shard: 分片名称
//   - vector: 向量数组
//   - targetVector: 目标向量数组
//   - distance: 距离阈值
//   - limit: 结果限制数量
//   - filters: 本地过滤器
//   - keywordRanking: 关键词排序参数
//   - sort: 排序规则
//   - cursor: 游标参数
//   - groupBy: 分组参数
//   - additional: 附加属性
//   - targetCombination: 目标组合
//   - properties: 属性列表
// 返回值: 对象数组、分布数组和错误信息
func (c *RemoteIndex) SearchShard(ctx context.Context, host, index, shard string,
	vector []models.Vector,
	targetVector []string,
	distance float32,
	limit int,
	filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking,
	sort []filters.Sort,
	cursor *filters.Cursor,
	groupBy *searchparams.GroupBy,
	additional additional.Properties,
	targetCombination *dto.TargetCombination,
	properties []string,
) ([]*storobj.Object, []float32, error) {
	// 序列化搜索参数
	body, err := clusterapi.IndicesPayloads.SearchParams.
		Marshal(vector, targetVector, distance, limit, filters, keywordRanking, sort, cursor, groupBy, additional, targetCombination, properties)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal request payload: %w", err)
	}
	// 构建搜索请求
	req, err := setupRequest(ctx, http.MethodPost, host,
		fmt.Sprintf("/indices/%s/shards/%s/objects/_search", index, shard),
		"", bytes.NewReader(body))
	if err != nil {
		return nil, nil, fmt.Errorf("create http request: %w", err)
	}
	// 设置内容类型头部
	clusterapi.IndicesPayloads.SearchParams.SetContentTypeHeaderReq(req)

	// 发送搜索请求
	resp := &searchShardResp{}
	err = c.doWithCustomMarshaller(c.timeoutUnit*QUERY_TIMEOUT_VALUE, req, body, resp.decode, successCode, MAX_RETRIES)
	return resp.Objects, resp.Distributions, err
}

// searchShardResp 搜索分片响应结构体
type searchShardResp struct {
	Objects       []*storobj.Object // 搜索结果对象数组
	Distributions []float32         // 分布数组
}

// decode 解码搜索响应数据
// 参数:
//   - data: 响应数据字节
// 返回值: 错误信息
func (r *searchShardResp) decode(data []byte) (err error) {
	r.Objects, r.Distributions, err = clusterapi.IndicesPayloads.SearchResults.Unmarshal(data)
	return err
}

// aggregateResp 聚合响应结构体
type aggregateResp struct {
	Result *aggregation.Result // 聚合结果
}

// decode 解码聚合响应数据
// 参数:
//   - data: 响应数据字节
// 返回值: 错误信息
func (r *aggregateResp) decode(data []byte) (err error) {
	r.Result, err = clusterapi.IndicesPayloads.AggregationResult.Unmarshal(data)
	return err
}

// Aggregate 在指定分片上执行聚合操作
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - index: 索引名称
//   - shard: 分片名称
//   - params: 聚合参数
// 返回值: 聚合结果和错误信息
func (c *RemoteIndex) Aggregate(ctx context.Context, hostName, index,
	shard string, params aggregation.Params,
) (*aggregation.Result, error) {
	// 序列化聚合参数
	body, err := clusterapi.IndicesPayloads.AggregationParams.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshal request payload: %w", err)
	}
	// 构建聚合请求
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects/_aggregations", index, shard),
		"", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create http request: %w", err)
	}
	// 设置内容类型头部
	clusterapi.IndicesPayloads.AggregationParams.SetContentTypeHeaderReq(req)

	// 发送聚合请求
	resp := &aggregateResp{}
	err = c.doWithCustomMarshaller(c.timeoutUnit*QUERY_TIMEOUT_VALUE, req, body, resp.decode, successCode, MAX_RETRIES)
	return resp.Result, err
}

// FindUUIDs 根据过滤条件查找UUID
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - filters: 本地过滤器
//   - limit: 结果限制数量
// 返回值: UUID数组和错误信息
func (c *RemoteIndex) FindUUIDs(ctx context.Context, hostName, indexName,
	shardName string, filters *filters.LocalFilter, limit int,
) ([]strfmt.UUID, error) {
	// 序列化查找参数
	paramsBytes, err := clusterapi.IndicesPayloads.FindUUIDsParams.Marshal(filters, limit)
	if err != nil {
		return nil, errors.Wrap(err, "marshal request payload")
	}
	// 构建查找请求
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects/_find", indexName, shardName),
		"", bytes.NewReader(paramsBytes))
	if err != nil {
		return nil, errors.Wrap(err, "open http request")
	}

	// 设置内容类型头部
	clusterapi.IndicesPayloads.FindUUIDsParams.SetContentTypeHeaderReq(req)
	// 发送请求
	res, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	// 检查成功状态码
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return nil, errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	// 读取响应体
	resBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read body")
	}

	// 验证内容类型
	ct, ok := clusterapi.IndicesPayloads.FindUUIDsResults.CheckContentTypeHeader(res)
	if !ok {
		return nil, errors.Errorf("unexpected content type: %s", ct)
	}

	// 反序列化UUID结果
	uuids, err := clusterapi.IndicesPayloads.FindUUIDsResults.Unmarshal(resBytes)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal body")
	}
	return uuids, nil
}

// DeleteObjectBatch 批量删除对象
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - uuids: 要删除的UUID数组
//   - deletionTime: 删除时间
//   - dryRun: 是否为试运行模式
//   - schemaVersion: 模式版本号
// 返回值: 批量简单对象结果
func (c *RemoteIndex) DeleteObjectBatch(ctx context.Context, hostName, indexName, shardName string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) objects.BatchSimpleObjects {
	// 将模式版本号转换为字符串切片
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	// 序列化批量删除参数
	marshalled, err := clusterapi.IndicesPayloads.BatchDeleteParams.Marshal(uuids, deletionTime, dryRun)
	if err != nil {
		err := errors.Wrap(err, "marshal payload")
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}
	// 构建批量删除请求
	req, err := setupRequest(ctx, http.MethodDelete, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects", indexName, shardName),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
		bytes.NewReader(marshalled))
	if err != nil {
		err := errors.Wrap(err, "open http request")
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	// 设置内容类型头部
	clusterapi.IndicesPayloads.BatchDeleteParams.SetContentTypeHeaderReq(req)

	// 发送删除请求
	res, err := c.client.Do(req)
	if err != nil {
		err := errors.Wrap(err, "send http request")
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	defer res.Body.Close()
	// 检查成功状态码
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		err := errors.Errorf("unexpected status code %d (%s)", res.StatusCode, body)
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	// 验证内容类型
	if ct, ok := clusterapi.IndicesPayloads.BatchDeleteResults.
		CheckContentTypeHeader(res); !ok {
		err := errors.Errorf("unexpected content type: %s", ct)
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	// 读取响应体
	resBytes, err := io.ReadAll(res.Body)
	if err != nil {
		err := errors.Wrap(err, "ready body")
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	// 反序列化批量删除结果
	batchDeleteResults, err := clusterapi.IndicesPayloads.BatchDeleteResults.Unmarshal(resBytes)
	if err != nil {
		err := errors.Wrap(err, "unmarshal body")
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	return batchDeleteResults
}

// GetShardQueueSize 获取分片队列大小
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
// 返回值: 队列大小和错误信息
func (c *RemoteIndex) GetShardQueueSize(ctx context.Context,
	hostName, indexName, shardName string,
) (int64, error) {
	// 构建获取队列大小请求
	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/queuesize", indexName, shardName),
		"", nil)
	if err != nil {
		return 0, errors.Wrap(err, "open http request")
	}
	var size int64
	// 设置内容类型头部
	clusterapi.IndicesPayloads.GetShardQueueSizeParams.SetContentTypeHeaderReq(req)
	// 定义重试尝试函数
	try := func(ctx context.Context) (bool, error) {
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		// 检查响应状态码
		if code := res.StatusCode; code != http.StatusOK {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		// 读取响应体
		resBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return false, errors.Wrap(err, "read body")
		}

		// 验证内容类型
		ct, ok := clusterapi.IndicesPayloads.GetShardQueueSizeResults.CheckContentTypeHeader(res)
		if !ok {
			return false, errors.Errorf("unexpected content type: %s", ct)
		}

		// 反序列化队列大小
		size, err = clusterapi.IndicesPayloads.GetShardQueueSizeResults.Unmarshal(resBytes)
		if err != nil {
			return false, errors.Wrap(err, "unmarshal body")
		}
		return false, nil
	}
	// 执行带重试的请求
	return size, c.retry(ctx, MAX_RETRIES, try)
}

// GetShardStatus 获取分片状态
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
// 返回值: 状态字符串和错误信息
func (c *RemoteIndex) GetShardStatus(ctx context.Context,
	hostName, indexName, shardName string,
) (string, error) {
	// 构建获取分片状态请求
	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/status", indexName, shardName),
		"", nil)
	if err != nil {
		return "", errors.Wrap(err, "open http request")
	}
	var status string
	// 设置内容类型头部
	clusterapi.IndicesPayloads.GetShardStatusParams.SetContentTypeHeaderReq(req)
	// 定义重试尝试函数
	try := func(ctx context.Context) (bool, error) {
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		// 检查响应状态码
		if code := res.StatusCode; code != http.StatusOK {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		// 读取响应体
		resBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return false, errors.Wrap(err, "read body")
		}

		// 验证内容类型
		ct, ok := clusterapi.IndicesPayloads.GetShardStatusResults.CheckContentTypeHeader(res)
		if !ok {
			return false, errors.Errorf("unexpected content type: %s", ct)
		}

		// 反序列化状态信息
		status, err = clusterapi.IndicesPayloads.GetShardStatusResults.Unmarshal(resBytes)
		if err != nil {
			return false, errors.Wrap(err, "unmarshal body")
		}
		return false, nil
	}
	// 执行带重试的请求
	return status, c.retry(ctx, MAX_RETRIES, try)
}

// UpdateShardStatus 更新分片状态
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - targetStatus: 目标状态
//   - schemaVersion: 模式版本号
// 返回值: 错误信息
func (c *RemoteIndex) UpdateShardStatus(ctx context.Context, hostName, indexName, shardName,
	targetStatus string, schemaVersion uint64,
) error {
	// 序列化更新状态参数
	paramsBytes, err := clusterapi.IndicesPayloads.UpdateShardStatusParams.Marshal(targetStatus)
	if err != nil {
		return errors.Wrap(err, "marshal request payload")
	}
	// 将模式版本号转换为字符串切片
	value := []string{strconv.FormatUint(schemaVersion, 10)}

	// 定义重试尝试函数
	try := func(ctx context.Context) (bool, error) {
		// 构建更新状态请求
		req, err := setupRequest(ctx, http.MethodPost, hostName,
			fmt.Sprintf("/indices/%s/shards/%s/status", indexName, shardName),
			url.Values{replica.SchemaVersionKey: value}.Encode(),
			bytes.NewReader(paramsBytes))
		if err != nil {
			return false, fmt.Errorf("create http request: %w", err)
		}
		// 设置内容类型头部
		clusterapi.IndicesPayloads.UpdateShardStatusParams.SetContentTypeHeaderReq(req)

		// 发送请求
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		// 检查响应状态码
		if code := res.StatusCode; code != http.StatusOK {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}

		return false, nil
	}

	// 执行带重试的请求
	return c.retry(ctx, MAX_RETRIES, try)
}

// PutFile 上传文件到远程分片
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - fileName: 文件名
//   - payload: 文件内容读取器
// 返回值: 错误信息
func (c *RemoteIndex) PutFile(ctx context.Context, hostName, indexName,
	shardName, fileName string, payload io.ReadSeekCloser,
) error {
	// 确保在函数结束时关闭payload
	defer payload.Close()

	// 定义重试尝试函数
	try := func(ctx context.Context) (bool, error) {
		// 构建文件上传请求
		req, err := setupRequest(ctx, http.MethodPost, hostName,
			fmt.Sprintf("/indices/%s/shards/%s/files/%s", indexName, shardName, fileName),
			"", payload)
		if err != nil {
			return false, fmt.Errorf("create http request: %w", err)
		}
		// 设置内容类型头部
		clusterapi.IndicesPayloads.ShardFiles.SetContentTypeHeaderReq(req)
		// 发送请求
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		// 检查上传成功状态
		if code := res.StatusCode; code != http.StatusNoContent {
			shouldRetry := shouldRetry(code)
			if shouldRetry {
				// 重置文件读取位置以便重试
				_, err := payload.Seek(0, 0)
				shouldRetry = (err == nil)
			}
			body, _ := io.ReadAll(res.Body)
			return shouldRetry, fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		return false, nil
	}

	// 执行带重试的文件上传
	return c.retry(ctx, MAX_RETRIES, try)
}

// CreateShard 在远程节点上创建分片
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
// 返回值: 错误信息
func (c *RemoteIndex) CreateShard(ctx context.Context,
	hostName, indexName, shardName string,
) error {
	// 构建创建分片请求
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s", indexName, shardName),
		"", nil)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}
	// 定义重试尝试函数
	try := func(ctx context.Context) (bool, error) {
		// 发送创建请求
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		// 检查创建成功状态
		if code := res.StatusCode; code != http.StatusCreated {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		return false, nil
	}

	// 执行带重试的创建请求
	return c.retry(ctx, MAX_RETRIES, try)
}

// ReInitShard 重新初始化远程分片
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
// 返回值: 错误信息
func (c *RemoteIndex) ReInitShard(ctx context.Context,
	hostName, indexName, shardName string,
) error {
	// 构建重新初始化分片请求
	req, err := setupRequest(ctx, http.MethodPut, hostName,
		fmt.Sprintf("/indices/%s/shards/%s:reinit", indexName, shardName),
		"", nil)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}
	// 定义重试尝试函数
	try := func(ctx context.Context) (bool, error) {
		// 发送重新初始化请求
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		// 检查重新初始化成功状态
		if code := res.StatusCode; code != http.StatusNoContent {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)

		}
		return false, nil
	}

	// 执行带重试的重新初始化请求
	return c.retry(ctx, MAX_RETRIES, try)
}

// PauseFileActivity 暂停指定主机上集合分片副本的后台进程
// 注意：完成文件操作后应显式恢复后台进程
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - schemaVersion: 模式版本号
// 返回值: 错误信息
func (c *RemoteIndex) PauseFileActivity(ctx context.Context,
	hostName, indexName, shardName string, schemaVersion uint64,
) error {
	// 将模式版本号转换为字符串切片
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	// 构建暂停文件活动请求
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/background:pause", indexName, shardName),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
		nil,
	)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}

	// 定义重试尝试函数
	try := func(ctx context.Context) (bool, error) {
		// 发送暂停请求
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		// 检查暂停成功状态
		if code := res.StatusCode; code != http.StatusOK {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		return false, nil
	}
	// 执行带重试的暂停请求
	return c.retry(ctx, MAX_RETRIES, try)
}

// ResumeFileActivity 恢复指定主机上集合分片副本的后台进程
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
// 返回值: 错误信息
func (c *RemoteIndex) ResumeFileActivity(ctx context.Context,
	hostName, indexName, shardName string,
) error {
	// 构建恢复文件活动请求
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/background:resume", indexName, shardName),
		"", nil)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}

	// 定义重试尝试函数
	try := func(ctx context.Context) (bool, error) {
		// 发送恢复请求
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		// 检查恢复成功状态
		if code := res.StatusCode; code != http.StatusOK {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		return false, nil
	}
	// 执行带重试的恢复请求
	return c.retry(ctx, MAX_RETRIES, try)
}

// ListFiles 返回可用于获取分片数据的文件列表
// 返回的相对文件路径相对于分片的根目录
// indexName 是集合名称
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
// 返回值: 相对文件路径数组和错误信息
func (c *RemoteIndex) ListFiles(ctx context.Context,
	hostName, indexName, shardName string,
) ([]string, error) {
	// 构建列出文件请求
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/background:list", indexName, shardName),
		"", nil)
	if err != nil {
		return []string{}, fmt.Errorf("create http request: %w", err)
	}

	var relativeFilePaths []string
	// 设置内容类型头部
	clusterapi.IndicesPayloads.ShardFilesResults.SetContentTypeHeaderReq(req)
	// 定义重试尝试函数
	try := func(ctx context.Context) (bool, error) {
		// 发送列出文件请求
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		// 检查成功状态码
		if code := res.StatusCode; code != http.StatusOK {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		// 读取响应体
		resBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return false, errors.Wrap(err, "read body")
		}

		// 反序列化文件路径列表
		relativeFilePaths, err = clusterapi.IndicesPayloads.ShardFilesResults.Unmarshal(resBytes)
		if err != nil {
			return false, errors.Wrap(err, "unmarshal body")
		}
		return false, nil
	}
	// 执行带重试的列出文件请求
	return relativeFilePaths, c.retry(ctx, MAX_RETRIES, try)
}

// GetFileMetadata 返回相对于分片根目录的文件元数据信息
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - relativeFilePath: 相对于分片根目录的文件路径
// 返回值: 文件元数据和错误信息
func (c *RemoteIndex) GetFileMetadata(ctx context.Context, hostName, indexName,
	shardName, relativeFilePath string,
) (file.FileMetadata, error) {
	// 构建获取文件元数据请求
	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/files:metadata/%s", indexName, shardName, relativeFilePath),
		"", nil)
	if err != nil {
		return file.FileMetadata{}, fmt.Errorf("create http request: %w", err)
	}

	// 设置内容类型头部
	clusterapi.IndicesPayloads.ShardFiles.SetContentTypeHeaderReq(req)

	var md file.FileMetadata

	// 定义重试尝试函数
	try := func(ctx context.Context) (bool, error) {
		// 发送获取元数据请求
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}

		// 检查成功状态码
		if res.StatusCode != http.StatusOK {
			defer res.Body.Close()
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(res.StatusCode), fmt.Errorf(
				"unexpected status code %d (%s)", res.StatusCode, body)
		}

		// 读取响应体
		resBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return false, errors.Wrap(err, "read body")
		}

		// 反序列化文件元数据
		md, err = clusterapi.IndicesPayloads.ShardFileMetadataResults.Unmarshal(resBytes)
		if err != nil {
			return false, errors.Wrap(err, "unmarshal body")
		}

		return false, nil
	}
	// 执行带重试的获取元数据请求
	return md, c.retry(ctx, MAX_RETRIES, try)
}

// GetFile 获取文件内容，调用者必须在无错误返回时关闭返回的io.ReadCloser
// indexName 是集合名称。relativeFilePath 是相对于分片根目录的文件路径
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - relativeFilePath: 相对于分片根目录的文件路径
// 返回值: 文件读取器和错误信息
func (c *RemoteIndex) GetFile(ctx context.Context, hostName, indexName,
	shardName, relativeFilePath string,
) (io.ReadCloser, error) {
	// 构建获取文件请求
	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/files/%s", indexName, shardName, relativeFilePath),
		"", nil)
	if err != nil {
		return nil, fmt.Errorf("create http request: %w", err)
	}
	// 设置内容类型头部
	clusterapi.IndicesPayloads.ShardFiles.SetContentTypeHeaderReq(req)
	var file io.ReadCloser
	// 定义重试尝试函数
	try := func(ctx context.Context) (bool, error) {
		// 发送获取文件请求
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}

		// 检查成功状态码
		if res.StatusCode != http.StatusOK {
			defer res.Body.Close()
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(res.StatusCode), fmt.Errorf(
				"unexpected status code %d (%s)", res.StatusCode, body)
		}

		// 返回文件内容读取器
		file = res.Body
		return false, nil
	}
	// 执行带重试的获取文件请求
	return file, c.retry(ctx, MAX_RETRIES, try)
}

// AddAsyncReplicationTargetNode 为目标主机配置并启动异步复制
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - targetNodeOverride: 目标节点覆盖配置
//   - schemaVersion: 模式版本号
// 返回值: 错误信息
func (c *RemoteIndex) AddAsyncReplicationTargetNode(
	ctx context.Context,
	hostName, indexName, shardName string,
	targetNodeOverride additional.AsyncReplicationTargetNodeOverride,
	schemaVersion uint64,
) error {
	// 序列化目标节点覆盖配置
	body, err := clusterapi.IndicesPayloads.AsyncReplicationTargetNode.Marshal(targetNodeOverride)
	if err != nil {
		return fmt.Errorf("marshal target node override: %w", err)
	}
	// 将模式版本号转换为字符串切片
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	// 构建添加异步复制目标节点请求
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		AsyncReplicationTargetNodeEndpoint(indexName, shardName),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}
	// 设置内容类型头部
	clusterapi.IndicesPayloads.AsyncReplicationTargetNode.SetContentTypeHeaderReq(req)

	// 定义重试尝试函数
	try := func(ctx context.Context) (bool, error) {
		// 发送添加目标节点请求
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		// 检查添加成功状态
		if code := res.StatusCode; code != http.StatusOK {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		return false, nil
	}
	// 执行带重试的添加请求
	return c.retry(ctx, MAX_RETRIES, try)
}

// RemoveAsyncReplicationTargetNode 移除异步复制的目标节点覆盖配置
// 参数:
//   - ctx: 上下文对象
//   - hostName: 目标主机名
//   - indexName: 索引名称
//   - shardName: 分片名称
//   - targetNodeOverride: 目标节点覆盖配置
// 返回值: 错误信息
func (c *RemoteIndex) RemoveAsyncReplicationTargetNode(
	ctx context.Context,
	hostName, indexName, shardName string,
	targetNodeOverride additional.AsyncReplicationTargetNodeOverride,
) error {
	// 序列化目标节点覆盖配置
	body, err := clusterapi.IndicesPayloads.AsyncReplicationTargetNode.Marshal(targetNodeOverride)
	if err != nil {
		return fmt.Errorf("marshal target node override: %w", err)
	}

	// 构建移除异步复制目标节点请求
	req, err := setupRequest(ctx, http.MethodDelete, hostName,
		AsyncReplicationTargetNodeEndpoint(indexName, shardName),
		"", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}
	// 设置内容类型头部
	clusterapi.IndicesPayloads.AsyncReplicationTargetNode.SetContentTypeHeaderReq(req)

	// 定义重试尝试函数
	try := func(ctx context.Context) (bool, error) {
		// 发送移除目标节点请求
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		// 检查移除成功状态
		if code := res.StatusCode; code != http.StatusNoContent {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		return false, nil
	}
	// 执行带重试的移除请求
	return c.retry(ctx, MAX_RETRIES, try)
}

// setupRequest 是一个简单的辅助函数，用于创建具有指定方法、主机、路径、查询和主体的新HTTP请求
// 注意：如果不需要查询参数可以留空，主体可以为nil。此函数不发送请求，只是创建请求对象
// 参数:
//   - ctx: 上下文对象
//   - method: HTTP方法
//   - host: 主机地址
//   - path: 请求路径
//   - query: 查询字符串
//   - body: 请求主体
// 返回值: HTTP请求对象和错误信息
func setupRequest(
	ctx context.Context,
	method, host, path, query string,
	body io.Reader,
) (*http.Request, error) {
	// 构建完整的URL
	url := url.URL{Scheme: "http", Host: host, Path: path, RawQuery: query}
	// 创建带上下文的HTTP请求
	req, err := http.NewRequestWithContext(ctx, method, url.String(), body)
	if err != nil {
		return nil, fmt.Errorf("create http request: %w", err)
	}
	return req, nil
}
