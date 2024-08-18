package loaders

import "caribou-go/src/utils"

const (
	SolverInputAverageMemoryPowerDefault    = 100.0
	SolverInputPueDefault                   = 1.0
	SolverInputCfeDefault                   = 0.0
	SolverInputComputeCostDefault           = 100.0
	SolverInputInvocationCostDefault        = 100.0
	SolverInputTransmissionCostDefault      = 100.0
	SolverInputMaxCpuPowerDefault           = 0.0035
	SolverInputMinCpuPowerDefault           = 0.00074
	SolverInputSnsRequestCostDefault        = 0.50 / 1000000 // 0.50 USD per 1 million requests (At Ohio region)
	SolverInputDynamodbReadCostDefault      = 0.25 / 1000000 // 0.25 USD per 1 million read request unit (At Ohio region)
	SolverInputDynamodbWriteCostDefault     = 1.25 / 1000000 // 1.25 USD per 1 million write request unit (At Ohio region)
	SolverInputEcrMonthlyStorageCostDefault = 0.10           // 0.10 USD per 1 GB per month (At Ohio region)
)

type DataCenterLoader struct {
	DataCenterData map[string]interface{}
}

func NewDataCenterLoader(datacenterData map[string]interface{}) (*DataCenterLoader, error) {
	datacenterLoader := DataCenterLoader{DataCenterData: datacenterData}
	return &datacenterLoader, nil
}

func (d *DataCenterLoader) GetAverageMemoryPower(regionName string) float64 {
	return utils.Get(d.DataCenterData, SolverInputAverageMemoryPowerDefault, regionName, "average_memory_power")
}

func (d *DataCenterLoader) GetPue(regionName string) float64 {
	return utils.Get(d.DataCenterData, SolverInputPueDefault, regionName, "pue")
}

func (d *DataCenterLoader) GetCfe(regionName string) float64 {
	return utils.Get(d.DataCenterData, SolverInputCfeDefault, regionName, "cfe")
}

func (d *DataCenterLoader) GetMaxCpuPower(regionName string) float64 {
	return utils.Get(d.DataCenterData, SolverInputMaxCpuPowerDefault, regionName, "max_cpu_power_kWh")
}

func (d *DataCenterLoader) GetMinCpuPower(regionName string) float64 {
	return utils.Get(d.DataCenterData, SolverInputMinCpuPowerDefault, regionName, "min_cpu_power_kWh")
}

func (d *DataCenterLoader) GetSNSRequestCost(regionName string) float64 {
	return utils.Get(d.DataCenterData, SolverInputSnsRequestCostDefault, regionName, "sns_cost", "sns_cost")
}

func (d *DataCenterLoader) GetDynamoDBReadWriteCost(regionName string) (float64, float64) {
	dynamoDbCosts := utils.Get(d.DataCenterData, map[string]interface{}{}, regionName, "dynamodb_cost")
	return utils.Get(dynamoDbCosts, SolverInputDynamodbReadCostDefault, "read_cost"), utils.Get(dynamoDbCosts, SolverInputDynamodbWriteCostDefault, "write_cost")
}

func (d *DataCenterLoader) GetEcrStorageCost(regionName string) float64 {
	return utils.Get(d.DataCenterData, SolverInputEcrMonthlyStorageCostDefault, regionName, "ecr_cost", "storage_cost")
}

func (d *DataCenterLoader) GetComputeCost(regionName string, architecture string) float64 {
	return utils.Get(d.DataCenterData, SolverInputComputeCostDefault, regionName, "execution_cost", "compute_cost", architecture)
}

func (d *DataCenterLoader) GetInvocationCost(regionName string, architecture string) float64 {
	return utils.Get(d.DataCenterData, SolverInputInvocationCostDefault, regionName, "execution_cost", "invocation_cost", architecture)
}

func (d *DataCenterLoader) GetTransmissionCost(regionName string, intraProviderTransfer bool) float64 {
	var transferType string
	if intraProviderTransfer {
		transferType = "provider_data_transfer"
	} else {
		transferType = "global_data_transfer"
	}
	return utils.Get(d.DataCenterData, SolverInputTransmissionCostDefault, regionName, "transmission_cost", transferType)
}
