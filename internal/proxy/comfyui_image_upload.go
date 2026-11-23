package proxy

import (
	"bytes"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"path"
	"strings"
)

func isComfyUIImageUpload(request *http.Request) bool {
	return request != nil && request.URL != nil && request.Method == http.MethodPost &&
		(request.URL.Path == "/comfyui/upload/image" || request.URL.Path == "/comfyui/api/upload/image")
}

func (h *Handler) serveComfyUIImageUpload(w http.ResponseWriter, request *http.Request, info *routeInfo) {
	if request.URL.RawQuery != "" || request.URL.ForceQuery ||
		(request.URL.RawPath != "" && !validComfyUIResultPathEncoding(request.URL.RawPath)) ||
		len(request.Header.Values("Content-Type")) != 1 || request.Header.Get("Content-Encoding") != "" {
		writeJSONError(h.logger, w, http.StatusBadRequest, "invalid ComfyUI image upload request")
		return
	}
	if request.ContentLength > h.comfyUIImageUploadMaxBytes {
		writeJSONError(h.logger, w, http.StatusRequestEntityTooLarge, "ComfyUI image upload exceeds configured size limit")
		return
	}
	mediaType, parameters, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" || parameters["boundary"] == "" || len(parameters["boundary"]) > 70 {
		writeJSONError(h.logger, w, http.StatusUnsupportedMediaType, "ComfyUI image upload requires multipart/form-data")
		return
	}
	select {
	case h.comfyUIResultSlots <- struct{}{}:
		defer func() { <-h.comfyUIResultSlots }()
	default:
		writeJSONError(h.logger, w, http.StatusServiceUnavailable, "ComfyUI upload service busy")
		return
	}
	if request.Body == nil {
		writeJSONError(h.logger, w, http.StatusBadRequest, "ComfyUI image upload body is required")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, request.Body, h.comfyUIImageUploadMaxBytes))
	_ = request.Body.Close()
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSONError(h.logger, w, http.StatusRequestEntityTooLarge, "ComfyUI image upload exceeds configured size limit")
		} else {
			writeJSONError(h.logger, w, http.StatusBadRequest, "could not read ComfyUI image upload")
		}
		return
	}
	if !validComfyUIImageMultipart(body, parameters["boundary"]) {
		writeJSONError(h.logger, w, http.StatusBadRequest, "ComfyUI image upload requires one input image and supported fields")
		return
	}
	// Validate before forwarding any bytes. No temporary files or automatic write retries.
	forwarded := request.Clone(request.Context())
	forwarded.Body = io.NopCloser(bytes.NewReader(body))
	forwarded.GetBody = nil
	forwarded.ContentLength = int64(len(body))
	forwarded.TransferEncoding = nil
	forwarded.Trailer = nil
	*info = routeInfo{backendID: "comfyui-results", kind: routeInfoKindPath}
	if !h.ensureComfyUIStarted(w, request) {
		return
	}
	h.comfyUIResultReadProxy.ServeHTTP(w, forwarded)
}

func validComfyUIImageMultipart(body []byte, boundary string) bool {
	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	seen := make(map[string]bool, 4)
	for {
		part, err := reader.NextRawPart()
		if err == io.EOF {
			return seen["image"]
		}
		if err != nil || len(seen) >= 4 {
			return false
		}
		if !validComfyUIImageMultipartPart(part, seen) {
			return false
		}
	}
}

func validComfyUIImageMultipartPart(part *multipart.Part, seen map[string]bool) bool {
	name := part.FormName()
	if seen[name] || part.Header.Get("Content-Transfer-Encoding") != "" {
		return false
	}
	seen[name] = true
	if name == "image" {
		return validComfyUIImagePart(part)
	}
	return validComfyUIImageField(part, name)
}

func validComfyUIImagePart(part *multipart.Part) bool {
	_, disposition, err := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
	filename := disposition["filename"]
	if err != nil || filename == "" || len(filename) > 255 || path.Base(filename) != filename ||
		strings.Contains(filename, "..") || strings.ContainsAny(filename, "\\:\x00\r\n") {
		return false
	}
	switch strings.ToLower(path.Ext(filename)) {
	case ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff", ".avif":
	default:
		return false
	}
	prefix := make([]byte, 512)
	n, err := io.ReadFull(part, prefix)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return false
	}
	if n == 0 || !strings.HasPrefix(http.DetectContentType(prefix[:n]), "image/") {
		return false
	}
	_, err = io.Copy(io.Discard, part)
	return err == nil
}

func validComfyUIImageField(part *multipart.Part, name string) bool {
	if part.FileName() != "" || (name != "type" && name != "subfolder" && name != "overwrite") {
		return false
	}
	value, err := io.ReadAll(io.LimitReader(part, 2049))
	if err != nil || len(value) > 2048 {
		return false
	}
	text := string(value)
	switch name {
	case "type":
		return text == "input"
	case "overwrite":
		return text == "true" || text == "false" || text == "1" || text == "0"
	case "subfolder":
		return text == "" || (path.Clean(text) == text && !strings.HasPrefix(text, "/") && text != ".." &&
			!strings.HasPrefix(text, "../") && !strings.ContainsAny(text, "\\:\x00\r\n"))
	default:
		return false
	}
}
