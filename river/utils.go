package river

import (
	"bytes"

	"github.com/PuerkitoBio/goquery"
)

func HtmlStrip(input interface{}) (string, error) {
	buf := bytes.NewBuffer(nil)
	buf.Reset()
	switch value := input.(type) {
	case []byte:
		buf.Write(value)
	case string:
		buf.WriteString(value)
	default:
	}
	doc, e := goquery.NewDocumentFromReader(buf)
	if e != nil {
		return "", e
	}
	return doc.Text(), nil
}
