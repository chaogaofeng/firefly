package dai

import (
	"context"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"testing"
)

func TestRest(t *testing.T) {

	ctx := context.Background()

	client := resty.New().SetBaseURL("http://10.1.120.48:8000")
	body := map[string]interface{}{
		"TaskID": "1234567",
	}
	var result fftypes.JSONObject
	res, err := client.R().
		SetResult(&result).
		SetContext(ctx).
		SetBody(body).
		Post("/mpc/v1/getTaskByID")
	fmt.Println(res)
	fmt.Println(err)
	fmt.Println(result.GetObjectArray("msg")[0].GetInt64("taskStatus"))
}
