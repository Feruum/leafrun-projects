package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	documentcore "github.com/leafrun/leafrun-document-core"
	"github.com/leafrun/leafrun-projects/internal/store"
)

type fileKey struct {
	tree      string
	fontIndex int
	path      string
}

type stagedSnapshot struct {
	core    *documentcore.Snapshot
	content string
}

func (r *Runtime) registerSnapshot(ctx context.Context, scope storagev1.Scope, revisionID, snapshotID, idempotencyKey string, actor projectsv1.Principal) (projectsv1.Revision, error) {
	if !projectsv1.ValidScope(scope) || !projectsv1.ValidIdentifier(revisionID) || !projectsv1.ValidIdentifier(snapshotID) {
		return projectsv1.Revision{}, fault(400, "invalid_revision", "scope, revision and Storage snapshot are required")
	}
	base, err := r.restoreSnapshot(ctx, scope, snapshotID)
	if err != nil {
		return projectsv1.Revision{}, err
	}
	defer base.core.Close()
	manifest, err := documentcore.ReadProject(ctx, base.core.FS())
	if err != nil {
		return projectsv1.Revision{}, coreFault(err)
	}
	coreManifest := base.core.Manifest()
	descriptors := make([]projectsv1.TargetDescriptor, 0, len(manifest.Targets))
	for _, target := range manifest.Targets {
		if err = ctx.Err(); err != nil {
			return projectsv1.Revision{}, err
		}
		descriptor, describeErr := documentcore.DescribeTarget(ctx, base.core.FS(), manifest, coreManifest, target.ID, "")
		if describeErr != nil {
			return projectsv1.Revision{}, coreFault(describeErr)
		}
		encoded, marshalErr := json.Marshal(descriptor)
		if marshalErr != nil {
			return projectsv1.Revision{}, fault(500, "descriptor_invalid", "Core returned an unsupported target descriptor")
		}
		var wire projectsv1.TargetDescriptor
		if unmarshalErr := jsonvalue.Unmarshal(encoded, &wire, projectsv1.ResponseLimit); unmarshalErr != nil {
			return projectsv1.Revision{}, fault(500, "descriptor_invalid", "Core target descriptor exceeds the Projects contract")
		}
		descriptors = append(descriptors, wire)
	}
	input := projectsv1.Revision{Scope: scope, Revision: revisionID, SnapshotID: snapshotID, ContentID: base.content, Targets: descriptors}
	metadata, err := json.Marshal(input)
	if err != nil || int64(len(metadata)) > projectsv1.ResponseLimit {
		return projectsv1.Revision{}, fault(413, "revision_too_large", "verified project descriptors exceed the 12 MiB Projects limit")
	}
	revision, err := r.Store.RegisterRevision(ctx, store.RegisterRevisionInput{Actor: actor, Revision: input, IdempotencyKey: idempotencyKey})
	if err != nil {
		return projectsv1.Revision{}, storeFault(err)
	}
	return revision, nil
}

func (r *Runtime) restoreSnapshot(ctx context.Context, scope storagev1.Scope, snapshotID string) (stagedSnapshot, error) {
	response, err := r.Storage.GetSnapshot(ctx, snapshotID)
	if err != nil {
		return stagedSnapshot{}, storageFault(err, "snapshot_unavailable")
	}
	if response.SnapshotID != snapshotID || response.Scope != scope || response.Manifest.ID == "" {
		return stagedSnapshot{}, fault(409, "snapshot_binding_mismatch", "Storage snapshot scope or identity does not match the requested project")
	}
	plan, err := renderwire.PlanFiles(response.Manifest)
	if err != nil {
		return stagedSnapshot{}, fault(422, "invalid_snapshot_manifest", "Storage returned a snapshot manifest outside the supported contract")
	}
	var coreManifest documentcore.SnapshotManifest
	manifestJSON, err := json.Marshal(response.Manifest)
	if err != nil || jsonvalue.Unmarshal(manifestJSON, &coreManifest, projectsv1.ResponseLimit) != nil || documentcore.ValidateSnapshotManifest(coreManifest) != nil {
		return stagedSnapshot{}, fault(422, "invalid_snapshot_manifest", "Storage snapshot manifest failed Core content validation")
	}
	contentID, err := documentcore.SnapshotContentID(coreManifest)
	if err != nil || contentID != response.Manifest.ID {
		return stagedSnapshot{}, fault(422, "invalid_snapshot_manifest", "Storage snapshot content identity is invalid")
	}
	refs := make(map[fileKey]string, len(response.Resources))
	for _, ref := range response.Resources {
		key := fileKey{tree: ref.Tree, fontIndex: ref.FontIndex, path: ref.Path}
		if ref.ResourceID == "" || !projectsv1.ValidIdentifier(ref.ResourceID) {
			return stagedSnapshot{}, fault(422, "invalid_snapshot_bindings", "Storage snapshot has an invalid resource binding")
		}
		if _, duplicate := refs[key]; duplicate {
			return stagedSnapshot{}, fault(422, "invalid_snapshot_bindings", "Storage snapshot has duplicate resource bindings")
		}
		refs[key] = ref.ResourceID
	}
	if len(refs) != len(plan) {
		return stagedSnapshot{}, fault(422, "invalid_snapshot_bindings", "Storage snapshot resource bindings do not match its manifest")
	}
	for _, file := range plan {
		if _, ok := refs[fileKey{tree: file.Tree, fontIndex: file.FontIndex, path: file.Path}]; !ok {
			return stagedSnapshot{}, fault(422, "invalid_snapshot_bindings", "Storage snapshot resource bindings do not match its manifest")
		}
	}
	metadata := make(map[string]storagev1.Resource)
	for _, resourceID := range refs {
		if _, seen := metadata[resourceID]; seen {
			continue
		}
		resource, metadataErr := r.Storage.GetResource(ctx, resourceID)
		if metadataErr != nil {
			return stagedSnapshot{}, storageFault(metadataErr, "snapshot_resource_unavailable")
		}
		if resource.ResourceID != resourceID || resource.Scope != scope || resource.Class != storagev1.ClassResource {
			return stagedSnapshot{}, fault(422, "invalid_snapshot_bindings", "Storage resource is outside the snapshot scope or class")
		}
		metadata[resourceID] = resource
	}
	for _, file := range plan {
		resourceID := refs[fileKey{tree: file.Tree, fontIndex: file.FontIndex, path: file.Path}]
		resource := metadata[resourceID]
		if resource.SHA256 != file.SHA256 || resource.SizeBytes != file.Size {
			return stagedSnapshot{}, fault(422, "snapshot_resource_mismatch", "Storage resource metadata differs from its immutable manifest")
		}
	}

	stageRoot, err := os.MkdirTemp(r.Config.TempDir, "leafrun-projects-source-")
	if err != nil {
		return stagedSnapshot{}, fault(503, "snapshot_staging_unavailable", "Projects private snapshot staging is unavailable")
	}
	defer os.RemoveAll(stageRoot)
	projectRoot := filepath.Join(stageRoot, "project")
	if err = os.Mkdir(projectRoot, 0o700); err != nil {
		return stagedSnapshot{}, fault(503, "snapshot_staging_unavailable", "Projects private snapshot staging is unavailable")
	}
	if err = createDirs(projectRoot, response.Manifest.Directories); err != nil {
		return stagedSnapshot{}, fault(422, "invalid_snapshot_manifest", "Storage manifest contains a path not supported by this host")
	}
	externalRoots := make(map[int]string)
	for index, font := range response.Manifest.Fonts {
		if font.InProject {
			continue
		}
		root := filepath.Join(stageRoot, "fonts", strconv.Itoa(index))
		if err = os.MkdirAll(root, 0o700); err != nil {
			return stagedSnapshot{}, fault(503, "snapshot_staging_unavailable", "Projects private snapshot staging is unavailable")
		}
		if err = createDirs(root, font.Directories); err != nil {
			return stagedSnapshot{}, fault(422, "invalid_snapshot_manifest", "Storage font manifest contains a path not supported by this host")
		}
		externalRoots[index] = root
	}
	for _, file := range plan {
		key := fileKey{tree: file.Tree, fontIndex: file.FontIndex, path: file.Path}
		root := projectRoot
		if file.Tree == "font" {
			root = externalRoots[file.FontIndex]
		}
		if root == "" {
			return stagedSnapshot{}, fault(422, "invalid_snapshot_bindings", "Storage snapshot references an unknown external font tree")
		}
		destination, pathErr := safeStagedPath(root, file.Path)
		if pathErr != nil {
			return stagedSnapshot{}, fault(422, "invalid_snapshot_manifest", "Storage manifest contains a path not supported by this host")
		}
		if err = r.copyVerifiedResource(ctx, refs[key], file.Size, file.SHA256, destination); err != nil {
			return stagedSnapshot{}, err
		}
	}
	externalFilesystems := make(map[int]fs.FS, len(externalRoots))
	for index, root := range externalRoots {
		externalFilesystems[index] = os.DirFS(root)
	}
	limits := documentcore.SnapshotLimits{MaxFiles: 10000, MaxTotalBytes: 512 << 20, MaxFileBytes: 128 << 20}
	coreSnapshot, err := documentcore.RestoreSnapshot(ctx, documentcore.SnapshotBundle{Manifest: coreManifest, ProjectFS: os.DirFS(projectRoot), ExternalFontFS: externalFilesystems}, documentcore.RestoreSnapshotOptions{TempDir: r.Config.TempDir, Limits: limits})
	if err != nil {
		return stagedSnapshot{}, coreFault(err)
	}
	if coreSnapshot.Manifest().ID != response.Manifest.ID {
		_ = coreSnapshot.Close()
		return stagedSnapshot{}, fault(422, "snapshot_content_mismatch", "restored Storage snapshot content identity changed")
	}
	if err = os.RemoveAll(stageRoot); err != nil {
		_ = coreSnapshot.Close()
		return stagedSnapshot{}, fault(503, "snapshot_staging_unavailable", "Projects private snapshot staging could not be removed")
	}
	return stagedSnapshot{core: coreSnapshot, content: contentID}, nil
}

func createDirs(root string, directories []string) error {
	for _, directory := range directories {
		localized, err := safeStagedPath(root, directory)
		if err != nil {
			return err
		}
		if err = os.MkdirAll(localized, 0o700); err != nil {
			return err
		}
	}
	return nil
}

func safeStagedPath(root, name string) (string, error) {
	if !renderwire.SafeLogicalPath(name) {
		return "", errors.New("invalid logical path")
	}
	localized, err := filepath.Localize(name)
	if err != nil {
		return "", err
	}
	full := filepath.Join(root, localized)
	relative, err := filepath.Rel(root, full)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("logical path escaped the staging root")
	}
	return full, nil
}

func (r *Runtime) copyVerifiedResource(ctx context.Context, resourceID string, expectedSize int64, expectedHash, path string) error {
	response, err := r.Storage.ResourceContent(ctx, resourceID)
	if err != nil {
		return storageFault(err, "snapshot_resource_unavailable")
	}
	if response == nil || response.Body == nil {
		return fault(503, "snapshot_resource_unavailable", "Storage returned no resource body")
	}
	if response.StatusCode != http.StatusOK || response.ContentLength >= 0 && response.ContentLength != expectedSize {
		_ = response.Body.Close()
		return fault(503, "snapshot_resource_unavailable", "Storage resource response differs from its descriptor")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = response.Body.Close()
		return fault(503, "snapshot_staging_unavailable", "Projects private snapshot staging is unavailable")
	}
	digester := sha256.New()
	limited := &io.LimitedReader{R: response.Body, N: expectedSize + 1}
	written, copyErr := io.Copy(io.MultiWriter(file, digester), limited)
	bodyCloseErr := response.Body.Close()
	fileCloseErr := file.Close()
	if copyErr != nil || bodyCloseErr != nil || fileCloseErr != nil {
		return fault(503, "snapshot_resource_unavailable", "Storage resource bytes could not be verified")
	}
	if written != expectedSize || hex.EncodeToString(digester.Sum(nil)) != expectedHash {
		return fault(422, "snapshot_resource_mismatch", "Storage resource bytes differ from their immutable manifest")
	}
	return ctx.Err()
}

func coreFault(err error) error {
	var validation *documentcore.ProjectValidationError
	if errors.As(err, &validation) {
		field := projectsv1.FieldError{File: validation.File, Pointer: validation.Pointer, Code: validation.Code, Message: validation.Message}
		return faultFields(422, validation.Code, validation.Message, []projectsv1.FieldError{field})
	}
	return fault(422, "invalid_project", "Core rejected the immutable project snapshot")
}

func storageFault(err error, fallback string) error {
	var httpErr *storagev1.HTTPError
	if errors.As(err, &httpErr) {
		switch {
		case httpErr.Status == http.StatusUnauthorized:
			return fault(503, "storage_credential_rejected", "Projects Storage credential was rejected")
		case httpErr.Status == http.StatusForbidden:
			return fault(403, "forbidden", "Storage denied access to the project snapshot")
		case httpErr.Status == http.StatusNotFound:
			return fault(404, fallback, "Storage snapshot or resource was not found")
		case httpErr.Status == http.StatusGone:
			return fault(410, fallback, "Storage snapshot or resource is no longer available")
		case httpErr.Status == http.StatusConflict:
			return fault(409, fallback, "Storage snapshot or resource is not ready")
		}
	}
	return fault(503, "storage_unavailable", "Storage could not verify the immutable project snapshot")
}
