package parser

import (
	"fmt"
	"html"
	"regexp"
	"strings"
)

var htmlTagRegex = regexp.MustCompile("<[^>]*>")
var zeroWidthRegex = regexp.MustCompile("[\u200b\u200c\u200d\ufeff]")
var whitespaceRegex = regexp.MustCompile(`\s+`)

// NormalizeHTML strips tags, decodes html entities, removes zero-width characters, and normalizes spacing.
func NormalizeHTML(htmlContent string) string {
	// 1. Strip HTML tags
	clean := htmlTagRegex.ReplaceAllString(htmlContent, " ")

	// 2. Decode HTML entities (e.g. &nbsp; -> space, &amp; -> &)
	clean = html.UnescapeString(clean)

	// Replace non-breaking spaces with standard space
	clean = strings.ReplaceAll(clean, "\u00a0", " ")

	// 3. Remove zero-width obfuscation characters
	clean = zeroWidthRegex.ReplaceAllString(clean, "")

	// 4. Normalize multiple spaces/newlines to a single space
	clean = whitespaceRegex.ReplaceAllString(clean, " ")

	return strings.TrimSpace(clean)
}


// ExtractData extracts the OTP or verification URL using the configured method.
func ExtractData(normalizedText string, extractMethod string) (string, error) {
	switch strings.ToUpper(extractMethod) {
	case "OTP_EXTRACT":
		// Match 4 to 8 digit numerical codes (OTP)
		re := regexp.MustCompile(`\b\d{4,8}\b`)
		match := re.FindString(normalizedText)
		if match == "" {
			return "", fmt.Errorf("no numeric OTP code found matching 4-8 digits")
		}
		return match, nil

	case "NETFLIX_URL_EXTRACT":
		// Match Netflix password reset/login/verify URLs specifically
		re := regexp.MustCompile(`https://www\.netflix\.com/(?:password|account/update-primary-location|account/travel/verify|verifyemail|YourAccount)[^\s>\]]*`)
		match := re.FindString(normalizedText)
		if match == "" {
			return "", fmt.Errorf("no Netflix URL matching pattern found")
		}
		return html.UnescapeString(match), nil

	default:
		return "", fmt.Errorf("unsupported extraction method: %s", extractMethod)
	}
}
