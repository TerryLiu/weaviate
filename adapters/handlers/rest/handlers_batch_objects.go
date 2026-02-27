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

package rest

import (
	"errors"

	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
)

type batchObjectHandlers struct {
	manager             *objects.BatchManager
	metricRequestsTotal restApiRequestsTotal
}

// addObjects 处理批量创建对象的请求
// 参数:
//   - params: 批量创建对象的请求参数，包含HTTP请求上下文和请求体
//   - principal: 认证主体信息，用于权限验证
//
// 返回值:
//   - middleware.Responder: HTTP响应，可能包含成功创建的对象列表或错误信息
func (h *batchObjectHandlers) addObjects(params batch.BatchObjectsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	// 将认证主体信息添加到请求上下文中，用于后续的权限检查
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	
	// 获取复制属性配置，用于分布式环境下的数据一致性控制
	repl, err := getReplicationProperties(params.ConsistencyLevel, nil)
	if err != nil {
		// 记录请求失败的指标并返回400错误
		h.metricRequestsTotal.logError("", err)
		return batch.NewBatchObjectsCreateBadRequest().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// 调用manager执行批量对象创建操作
	// 参数说明:
	// - ctx: 带有认证信息的上下文
	// - principal: 认证主体
	// - params.Body.Objects: 要创建的对象列表
	// - params.Body.Fields: 指定要返回的字段
	// - repl: 复制配置参数
	objs, err := h.manager.AddObjects(ctx, principal,
		params.Body.Objects, params.Body.Fields, repl)
		
	if err != nil {
		// 记录请求失败的指标
		h.metricRequestsTotal.logError("", err)
		
		// 根据不同类型的错误返回相应的HTTP状态码
		switch {
		// 权限不足错误 - 返回403 Forbidden
		case errors.As(err, &autherrs.Forbidden{}):
			return batch.NewBatchObjectsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		// 用户输入无效错误 - 返回422 Unprocessable Entity
		case errors.As(err, &objects.ErrInvalidUserInput{}):
			return batch.NewBatchObjectsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		// 多租户相关错误 - 返回422 Unprocessable Entity
		case errors.As(err, &objects.ErrMultiTenancy{}):
			return batch.NewBatchObjectsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		// 其他未预期错误 - 返回500 Internal Server Error
		default:
			return batch.NewBatchObjectsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	// 记录请求成功的指标
	h.metricRequestsTotal.logOk("")
	
	// 返回成功响应，包含创建的对象信息
	return batch.NewBatchObjectsCreateOK().
		WithPayload(h.objectsResponse(objs))
}

func (h *batchObjectHandlers) objectsResponse(input objects.BatchObjects) []*models.ObjectsGetResponse {
	response := make([]*models.ObjectsGetResponse, len(input))
	for i, object := range input {
		var errorResponse *models.ErrorResponse
		status := models.ObjectsGetResponseAO2ResultStatusSUCCESS
		if object.Err != nil {
			errorResponse = errPayloadFromSingleErr(object.Err)
			status = models.ObjectsGetResponseAO2ResultStatusFAILED
		}

		object.Object.ID = object.UUID
		response[i] = &models.ObjectsGetResponse{
			Object: *object.Object,
			Result: &models.ObjectsGetResponseAO2Result{
				Errors: errorResponse,
				Status: &status,
			},
		}
	}

	return response
}

func (h *batchObjectHandlers) addReferences(params batch.BatchReferencesCreateParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	repl, err := getReplicationProperties(params.ConsistencyLevel, nil)
	if err != nil {
		h.metricRequestsTotal.logError("", err)
		return batch.NewBatchReferencesCreateBadRequest().
			WithPayload(errPayloadFromSingleErr(err))
	}

	references, err := h.manager.AddReferences(ctx, principal, params.Body, repl)
	if err != nil {
		h.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &autherrs.Forbidden{}):
			return batch.NewBatchReferencesCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.As(err, &objects.ErrInvalidUserInput{}):
			return batch.NewBatchReferencesCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.As(err, &objects.ErrMultiTenancy{}):
			return batch.NewBatchReferencesCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return batch.NewBatchReferencesCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.metricRequestsTotal.logOk("")
	return batch.NewBatchReferencesCreateOK().
		WithPayload(h.referencesResponse(references))
}

func (h *batchObjectHandlers) referencesResponse(input objects.BatchReferences) []*models.BatchReferenceResponse {
	response := make([]*models.BatchReferenceResponse, len(input))
	for i, ref := range input {
		var errorResponse *models.ErrorResponse
		var reference models.BatchReference

		status := models.BatchReferenceResponseAO1ResultStatusSUCCESS
		if ref.Err != nil {
			errorResponse = errPayloadFromSingleErr(ref.Err)
			status = models.BatchReferenceResponseAO1ResultStatusFAILED
		} else {
			reference.From = strfmt.URI(ref.From.String())
			reference.To = strfmt.URI(ref.To.String())
		}

		response[i] = &models.BatchReferenceResponse{
			BatchReference: reference,
			Result: &models.BatchReferenceResponseAO1Result{
				Errors: errorResponse,
				Status: &status,
			},
		}
	}

	return response
}

func (h *batchObjectHandlers) deleteObjects(params batch.BatchObjectsDeleteParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	repl, err := getReplicationProperties(params.ConsistencyLevel, nil)
	if err != nil {
		h.metricRequestsTotal.logError("", err)
		return batch.NewBatchObjectsDeleteBadRequest().
			WithPayload(errPayloadFromSingleErr(err))
	}

	tenant := getTenant(params.Tenant)

	res, err := h.manager.DeleteObjects(ctx, principal,
		params.Body.Match, params.Body.DeletionTimeUnixMilli, params.Body.DryRun, params.Body.Output, repl, tenant)
	if err != nil {
		h.metricRequestsTotal.logError("", err)
		if errors.As(err, &objects.ErrInvalidUserInput{}) {
			return batch.NewBatchObjectsDeleteUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		} else if errors.As(err, &objects.ErrMultiTenancy{}) {
			return batch.NewBatchObjectsDeleteUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		} else if errors.As(err, &autherrs.Forbidden{}) {
			return batch.NewBatchObjectsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		} else {
			return batch.NewBatchObjectsDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.metricRequestsTotal.logOk("")
	return batch.NewBatchObjectsDeleteOK().
		WithPayload(h.objectsDeleteResponse(res))
}

func (h *batchObjectHandlers) objectsDeleteResponse(input *objects.BatchDeleteResponse) *models.BatchDeleteResponse {
	var successful, failed int64
	output := input.Output
	var objects []*models.BatchDeleteResponseResultsObjectsItems0
	for _, obj := range input.Result.Objects {
		var errorResponse *models.ErrorResponse

		status := models.BatchDeleteResponseResultsObjectsItems0StatusSUCCESS
		if input.DryRun {
			status = models.BatchDeleteResponseResultsObjectsItems0StatusDRYRUN
		} else if obj.Err != nil {
			status = models.BatchDeleteResponseResultsObjectsItems0StatusFAILED
			errorResponse = errPayloadFromSingleErr(obj.Err)
			failed += 1
		} else {
			successful += 1
		}

		if output == verbosity.OutputMinimal &&
			(status == models.BatchDeleteResponseResultsObjectsItems0StatusSUCCESS ||
				status == models.BatchDeleteResponseResultsObjectsItems0StatusDRYRUN) {
			// only add SUCCESS and DRYRUN results if output is "verbose"
			continue
		}

		objects = append(objects, &models.BatchDeleteResponseResultsObjectsItems0{
			ID:     obj.UUID,
			Status: &status,
			Errors: errorResponse,
		})
	}

	deletionTimeUnixMilli := input.DeletionTime.UnixMilli()

	response := &models.BatchDeleteResponse{
		Match: &models.BatchDeleteResponseMatch{
			Class: input.Match.Class,
			Where: input.Match.Where,
		},
		DeletionTimeUnixMilli: &deletionTimeUnixMilli,
		DryRun:                &input.DryRun,
		Output:                &output,
		Results: &models.BatchDeleteResponseResults{
			Matches:    input.Result.Matches,
			Limit:      input.Result.Limit,
			Successful: successful,
			Failed:     failed,
			Objects:    objects,
		},
	}
	return response
}

func setupObjectBatchHandlers(api *operations.WeaviateAPI, manager *objects.BatchManager, metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) {
	h := &batchObjectHandlers{manager, newBatchRequestsTotal(metrics, logger)}

	api.BatchBatchObjectsCreateHandler = batch.
		BatchObjectsCreateHandlerFunc(h.addObjects)
	api.BatchBatchReferencesCreateHandler = batch.
		BatchReferencesCreateHandlerFunc(h.addReferences)
	api.BatchBatchObjectsDeleteHandler = batch.
		BatchObjectsDeleteHandlerFunc(h.deleteObjects)
}

type batchRequestsTotal struct {
	*restApiRequestsTotalImpl
}

func newBatchRequestsTotal(metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) restApiRequestsTotal {
	return &batchRequestsTotal{
		restApiRequestsTotalImpl: &restApiRequestsTotalImpl{newRequestsTotalMetric(metrics, "rest"), "rest", "batch", logger},
	}
}

func (e *batchRequestsTotal) logError(className string, err error) {
	switch {
	case errors.As(err, &errReplication{}):
		e.logUserError(className)
	case errors.As(err, &autherrs.Forbidden{}), errors.As(err, &objects.ErrInvalidUserInput{}):
		e.logUserError(className)
	case errors.As(err, &objects.ErrMultiTenancy{}):
		e.logUserError(className)
	default:
		if errors.As(err, &objects.ErrMultiTenancy{}) ||
			errors.As(err, &objects.ErrInvalidUserInput{}) ||
			errors.As(err, &autherrs.Forbidden{}) {
			e.logUserError(className)
		} else {
			e.logServerError(className, err)
		}
	}
}
