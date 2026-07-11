package parser

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jhillyerd/enmime"
)

func TestParseOriginalEMLFiles(t *testing.T) {
	files := []struct {
		filename string
		method   string
	}{
		{
			filename: "Kode verifikasimu.eml",
			method:   "OTP_EXTRACT",
		},
		{
			filename: "Netflix_ Kode masukmu.eml",
			method:   "OTP_EXTRACT",
		},
		{
			filename: "Selesaikan permintaanmu untuk mengatur ulang sandi.eml",
			method:   "NETFLIX_URL_EXTRACT",
		},
	}

	for _, tc := range files {
		t.Run(tc.filename, func(t *testing.T) {
			path := filepath.Join("../../email_sample", tc.filename)
			file, err := os.Open(path)
			if err != nil {
				t.Fatalf("Failed to open eml file at %s: %v", path, err)
			}
			defer file.Close()

			envelope, err := enmime.ReadEnvelope(file)
			if err != nil {
				t.Fatalf("Failed to parse EML envelope: %v", err)
			}

			// We prefer HTML body if present, fallback to Text body
			body := envelope.HTML
			if body == "" {
				body = envelope.Text
			}

			if body == "" {
				t.Fatalf("EML has empty HTML and Text body")
			}

			// For URL extraction, run on raw HTML body to preserve links.
			// Otherwise run on normalized body.
			var targetBody string
			if tc.method == "NETFLIX_URL_EXTRACT" {
				targetBody = body
			} else {
				targetBody = NormalizeHTML(body)
			}

			// Extract data
			extracted, err := ExtractData(targetBody, tc.method)
			if err != nil {
				t.Logf("Normalized text body was: %s", NormalizeHTML(body))
				t.Fatalf("Failed to extract data using %s: %v", tc.method, err)
			}

			t.Logf("File: %s", tc.filename)
			t.Logf("Extracted (%s): %s", tc.method, extracted)

			// Simple validations
			if tc.method == "OTP_EXTRACT" {
				if len(extracted) < 4 || len(extracted) > 8 {
					t.Errorf("Expected OTP length between 4 and 8, got: %s", extracted)
				}
			} else if tc.method == "NETFLIX_URL_EXTRACT" {
				if !strings.Contains(extracted, "netflix.com") {
					t.Errorf("Expected Netflix URL, got: %s", extracted)
				}
			}
		})
	}
}
