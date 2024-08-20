package loaders

type RegionViabilityLoader struct {
	AvailableRegions []string
}

func NewRegionViabilityLoader(availableRegions []interface{}) (*RegionViabilityLoader, error) {
	var availableRegionsStrs []string
	for _, region := range availableRegions {
		availableRegionsStrs = append(availableRegionsStrs, region.(string))
	}
	return &RegionViabilityLoader{availableRegionsStrs}, nil
}
