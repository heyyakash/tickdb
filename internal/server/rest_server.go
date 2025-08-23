package server

// import (
// 	"net/http"

// 	"github.com/gin-gonic/gin"
// 	"github.com/heyyakash/tickdb/pkg/model"
// )

// func handleDataPoint(c *gin.Context){
// 	var req model.DataPointRequest
// 	if err := c.ShouldBindJSON(&req); err!=nil{
// 		res := model.DataPointResponse{
// 			Accepted: 0,
// 			Rejected: 1,
// 			Error: "Invalid data",
// 		}
// 		c.JSON(http.StatusBadRequest, res)
// 		return
// 	}

// }
