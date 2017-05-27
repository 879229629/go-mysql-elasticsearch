package river

import (
	"bytes"
	"encoding/json"
	"strings"
)

const textKind = "text"

type Node struct {
	Kind  string `json:"kind"`
	Type  string `json:"type"`
	Text  string `json:"text"`
	Nodes Nodes  `json:"nodes"`
}

func (node *Node) String() string {
	var buffer bytes.Buffer
	if textKind == strings.ToLower(node.Kind) {
		buffer.WriteString(node.Text)
	}

	buffer.WriteString(node.Nodes.String())
	return buffer.String()
}

type Nodes []*Node

func (nodes Nodes) String() string {
	var buffer bytes.Buffer
	for _, node := range nodes {
		buffer.WriteString(node.String())
	}
	return buffer.String()
}

type Content struct {
	Nodes Nodes `json:"nodes"`
}

func (c *Content) String() string {
	return c.Nodes.String()
}

func ReplaceJsonFormatToText(input interface{}) (string, error) {
	switch value := input.(type) {
	case []byte:
		var c Content
		err := json.Unmarshal(value, &c)
		if err != nil {
			return "", err
		}
		return c.String(), nil
	case string:
		var c Content
		err := json.Unmarshal([]byte(value), &c)
		if err != nil {
			return "", err
		}
		return c.String(), nil
	default:
		return "", nil
	}
}
