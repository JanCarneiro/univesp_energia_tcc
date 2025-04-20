## Repositório criado para o TCC UNIVESP SALA 007 - GRUPO 001, ano 2025/ 01. 

#### Esse repositório contém os scripts construídos para o processo de ETL do projeto, trabalhando em um Data Lake construído com a arquitetura medalhão. 

A etapa presente no repositório consiste na extração dos dados usando requests e salvo em uma camada Bronze (Raw) identica aos dados originais, em seguida realizamos a consolidação e tratamento das bases com SQL, PySpark e Pandas e salvo na camada Silver, na próxima etapa realizamos a aplicação de algumas regras definidas no escopo do projeto utilizando as mesmas tecnologias descritas e armazenado em uma camada Gold, por fim carregamos os dados no Google Drive via API para o seguimento das próximas etapas do projeto.

Datasets:

Geração de energia por usina: https://dados.ons.org.br/dataset/geracao-usina-2  
Carga Energia: https://dados.ons.org.br/dataset/carga-energia  
Consumo de Energia: https://www.epe.gov.br/pt/publicacoes-dados-abertos/publicacoes/consumo-de-energia-eletrica  

Tecnologias e ferramentas usadas:  

Linguagens: Python e SQL  
Frameworks: Apache Spark (Com PySpark)  
IDE: VS Code  
Formatos de arquivos: Parquet, CSV, Excel e JSON  
Outros: WSL