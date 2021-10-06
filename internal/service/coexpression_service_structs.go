package service

type ServiceRequest struct {
	MolecularProfileA string
	MolecularProfileB string
	Threshold         float64
	Entrez_Gene_id    int64  `json:"entrezGeneId"`
	Sample_List_Id    string `json:"sampleListId"`
}

type CoExpression struct {
	Genetic_Entity_Identifier string  `json:"geneticEntityId"`
	Spearman                  float64 `json:"spearmansCorrelation"`
	PValue                    float64 `json:"pValue"`
}
type GeneticAlteration struct {
	Entrez_Gene_Identifier int64   `json:"Entrez_Gene_Identifier"`
	Sample_Identifier      string  `json:"Sample_Identifier"`
	Value                  float32 `json:"Value"`
}
