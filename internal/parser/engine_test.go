package parser

import (
	"testing"
)

func TestNormalizeHTML(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Strip basic tags",
			input:    "<html><body>Hello <b>World</b></body></html>",
			expected: "Hello World",
		},
		{
			name:     "Decode HTML entities",
			input:    "Hello&nbsp;World &amp; Friends",
			expected: "Hello World & Friends",
		},
		{
			name:     "Strip zero-width characters",
			input:    "H\u200be\u200cl\u200dl\ufeffo World",
			expected: "Hello World",
		},
		{
			name:     "Normalize multiple spaces and newlines",
			input:    "Hello   \n\n  World",
			expected: "Hello World",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeHTML(tt.input)
			if got != tt.expected {
				t.Errorf("NormalizeHTML() = %q, expected %q", got, tt.expected)
			}
		})
	}
}

func TestExtractData(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		method        string
		expected      string
		expectedError bool
	}{
		{
			name:          "Extract 6 digit OTP",
			input:         "Your Netflix verification code is 884712. Do not share it.",
			method:        "OTP_EXTRACT",
			expected:      "884712",
			expectedError: false,
		},
		{
			name:          "Extract 8 digit OTP",
			input:         "Your login code is 98765432.",
			method:        "OTP_EXTRACT",
			expected:      "98765432",
			expectedError: false,
		},
		{
			name:          "OTP Extract fail",
			input:         "No code here, only text.",
			method:        "OTP_EXTRACT",
			expected:      "",
			expectedError: true,
		},
		{
			name:          "Extract Netflix URL",
			input:         "Click here: https://www.netflix.com/password?g=12345 to activate.",
			method:        "NETFLIX_URL_EXTRACT",
			expected:      "https://www.netflix.com/password?g=12345",
			expectedError: false,
		},
		{
			name:          "Netflix URL Extract fail",
			input:         "Click here: https://google.com for google.",
			method:        "NETFLIX_URL_EXTRACT",
			expected:      "",
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractData(tt.input, tt.method)
			if (err != nil) != tt.expectedError {
				t.Errorf("ExtractData() error = %v, expectedError %v", err, tt.expectedError)
				return
			}
			if got != tt.expected {
				t.Errorf("ExtractData() = %q, expected %q", got, tt.expected)
			}
		})
	}
}
