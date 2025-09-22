package api

import (
    "context"
    "encoding/json"
    "errors"
    "github.com/aleph-zero/flutterdb/service/cluster"
    "github.com/aleph-zero/flutterdb/service/identity"
    "github.com/aleph-zero/flutterdb/service/index"
    "github.com/aleph-zero/flutterdb/service/membership"
    "github.com/aleph-zero/flutterdb/service/metastore"
    "github.com/aleph-zero/flutterdb/service/query"
    "github.com/go-chi/chi/v5"
    "github.com/go-chi/render"
    "net/http"
)

/* *** Identity API *** */

type IdentityHandler struct {
    service identity.Service
}

func (h *IdentityHandler) GetIdentity(w http.ResponseWriter, r *http.Request) {
    model := identity.Model{Identity: h.service.Identify()}
    data, err := json.Marshal(model)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)
    w.Write(data)
}

func NewIdentityHandler(svc identity.Service) IdentityHandler {
    return IdentityHandler{service: svc}
}

/* *** Query API *** */

type QueryHandler struct {
    service query.Service
}

func NewQueryHandler(svc query.Service) QueryHandler {
    return QueryHandler{service: svc}
}

func (h *QueryHandler) Query(w http.ResponseWriter, r *http.Request) {
    q := r.URL.Query().Get("q")
    result, err := h.service.Execute(r.Context(), q)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        return
    }

    response := &QueryResponse{result}
    render.Status(r, http.StatusOK)
    render.Render(w, r, response)
}

type QueryResponse struct {
    *query.QueryResult
}

func (q *QueryResponse) Render(w http.ResponseWriter, r *http.Request) error {
    return nil
}

/* *** Indexer API *** */

type IndexerHandler struct {
    service index.Service
}

func NewIndexerHandler(svc index.Service) IndexerHandler {
    return IndexerHandler{service: svc}
}

func (i *IndexerHandler) Index(w http.ResponseWriter, r *http.Request) {
    var documents []*index.Document
    processor := func(doc map[string]interface{}) error {
        documents = append(documents, &index.Document{Fields: doc})
        return nil
    }

    err := ProcessJsonStream(r, processor)
    if err != nil {
        render.Render(w, r, ErrInvalidRequest(err))
        return
    }

    index := r.Context().Value("index").(string)
    _, err = i.service.Index(r.Context(), index, documents)
    if err != nil {
        if errors.Is(err, metastore.Error{ErrorCode: metastore.NoSuchTable}) {
            render.Render(w, r, ErrInvalidRequest(err))
            return
        }
        render.Render(w, r, ErrInternalServerError(err))
        return
    }

    render.Status(r, http.StatusCreated)
    render.Render(w, r, &DocumentIndexResponse{})
}

type DocumentIndexResponse struct{}

func (d *DocumentIndexResponse) Render(w http.ResponseWriter, r *http.Request) error {
    return nil
}

func IndexContext(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        var index string
        if index = chi.URLParam(r, "index"); index == "" {
            render.Render(w, r, ErrInvalidRequest(errors.New("missing index name")))
            return
        }
        ctx := context.WithValue(r.Context(), "index", index)
        next.ServeHTTP(w, r.WithContext(ctx))
    })
}

/* *** Metastore API *** */

type MetastoreHandler struct {
    service metastore.Service
}

func (h *MetastoreHandler) Create(w http.ResponseWriter, r *http.Request) {
    data := &CreateTableRequest{}
    if err := render.Bind(r, data); err != nil {
        render.Render(w, r, ErrInvalidRequest(err))
        return
    }

    err := h.service.CreateTable(r.Context(), data.TableMetadata)
    if err != nil {
        if errors.Is(err, metastore.Error{ErrorCode: metastore.TableExists}) {
            render.Render(w, r, ErrInvalidRequest(err))
            return
        }
        render.Render(w, r, ErrInternalServerError(err))
        return
    }

    render.Status(r, http.StatusCreated)
    render.Render(w, r, &CreateTableResponse{data.TableMetadata})
}

func NewMetastoreHandler(svc metastore.Service) MetastoreHandler {
    return MetastoreHandler{service: svc}
}

type CreateTableRequest struct {
    *metastore.TableMetadata
}

func (c CreateTableRequest) Bind(r *http.Request) error {
    if c.TableMetadata == nil {
        return errors.New("missing required table definition")
    }
    return nil
}

type CreateTableResponse struct {
    *metastore.TableMetadata
}

func (c CreateTableResponse) Render(w http.ResponseWriter, r *http.Request) error {
    return nil
}

/* *** Membership API *** */

type MembershipHandler struct {
    service *membership.Membership
}

func (h *MembershipHandler) GetMembership(w http.ResponseWriter, r *http.Request) {
    data, err := json.Marshal(h.service.Members())
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)
    w.Write(data)
}

func NewMembershipHandler(svc *membership.Membership) MembershipHandler {
    return MembershipHandler{service: svc}
}

/* *** Cluster Info API *** */

type ClusterInfoHandler struct {
    service cluster.Service
}

func (h *ClusterInfoHandler) GetClusterInfo(w http.ResponseWriter, r *http.Request) {
    model := cluster.Model{
        Address: h.service.ClusterInfo().Address,
        Port:    h.service.ClusterInfo().Port,
    }
    data, err := json.Marshal(model)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)
    w.Write(data)
}

func NewClusterInfoHandler(svc cluster.Service) ClusterInfoHandler {
    return ClusterInfoHandler{service: svc}
}
