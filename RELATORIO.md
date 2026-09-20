# RELATÓRIO — Sistematização de Ciência de Dados II

**Aluna:** Luciane Siqueira Fernandes
**Matrícula:** 72500418
**Professor:** Romes Heriberto Pires de Araujo
**Disciplina:** Ciência de Dados II — Do Dado Bruto à Descoberta de Conhecimento

Este relatório documenta a aplicação do processo de KDD (Knowledge Discovery in
Databases) sobre dados do CAR/SICAR (Cadastro Ambiental Rural), estruturado pelas
etapas exigidas no trabalho.

## 1. Problema e dataset escolhido

O objetivo do trabalho foi aplicar um pipeline completo de KDD — seleção,
pré-processamento, análise exploratória, modelagem preditiva, modelagem descritiva e
interpretação — sobre um conjunto de dados público e volumoso, usando Apache Spark
para processamento distribuído.

Optou-se pela camada "Área do Imóvel" do CAR/SICAR (Cadastro Ambiental Rural),
referente ao estado da Bahia, por conexão com a área profissional de topografia e
geoprocessamento. Os dados foram obtidos diretamente do portal oficial
(https://consultapublica.car.gov.br/publico/estados/downloads), em formato shapefile.
Apenas a tabela de atributos (`.dbf`) foi utilizada — a geometria (`.shp`/`.shx`) foi
descartada, já que a análise não exige processamento espacial.

O dataset final contém **1.313.653 registros** (~184 MB em CSV), volume mais de 13
vezes acima do mínimo exigido (100 mil linhas), o que dispensou a necessidade de
combinar múltiplos estados.

Colunas principais utilizadas: `cod_imovel`, `mod_fiscal` (módulos fiscais),
`num_area` (área em hectares), `ind_status` (situação do cadastro), `ind_tipo` (tipo
de imóvel), `municipio`, `dat_criaca` e `dat_atuali` (datas de criação e última
atualização do cadastro).

## 2. Pré-processamento realizado

A extração da tabela de atributos do `.dbf` foi feita com um leitor próprio
(`src/leitor_dbf.py`), implementado sem dependências externas como geopandas/fiona,
executado via `src/etapa1_carregar_dbf.py`, que converte para `.csv` e já testa a
carga no Spark.

Na ingestão com PySpark (Etapa 2), identificou-se um ponto de atenção relevante:
as colunas de data (`dat_criaca`, `dat_atuali`) vêm no formato de texto brasileiro
(`dd/MM/yyyy`), e não no formato ISO que funções como `YEAR()` esperam por padrão.
Extrações diretas de ano retornavam `NULL` para 100% das linhas até a correção, feita
com `to_date(coluna, 'dd/MM/yyyy')` antes de qualquer operação temporal — um lembrete
de que a inspeção da representação bruta dos dados é indispensável antes de qualquer
análise que dependa dela.

## 3. Perguntas e respostas da análise exploratória (Spark SQL)

**Pergunta 1 — Quais municípios concentram a maior área cadastrada?**
Os 10 municípios com maior área total cadastrada são liderados por **Formosa do Rio
Preto** (1.910.207 ha, 5.509 imóveis, área média de 346,74 ha) e **São Desidério**
(1.511.448 ha, área média de 195,88 ha) — ambos no Oeste da Bahia, região conhecida
pela agricultura de larga escala (soja/grãos), o que explica a área média bem acima
da média geral do estado. Em contraste, **Pilão Arcado** aparece com o maior número
de imóveis do grupo (16.224) mas a menor área média (65,19 ha) — perfil típico do
semiárido baiano, de propriedades menores e mais numerosas.

**Pergunta 2 — Como se distribuem os imóveis por faixa de tamanho (módulos fiscais)?**
Usando o mesmo critério do INCRA aplicado na Etapa 5 (≤4 MF pequena, 4–15 MF média,
>15 MF grande), a distribuição revela uma concentração fundiária clara: a faixa
**Pequena** tem 1.283.109 imóveis (97,68% do total) mas responde por apenas 42,84%
da área total cadastrada (18.071.897 ha). Já a faixa **Grande**, com apenas 7.250
imóveis (0,55% do total), concentra 36,98% de toda a área (15.596.482 ha) — uma área
média de 2.151,24 ha por imóvel, contra 14,08 ha na faixa pequena. A faixa **Média**
(23.294 imóveis, 1,77%) responde pelos 20,20% restantes (8.520.160 ha). Ou seja: menos
de 1 em cada 100 imóveis concentra mais de um terço de toda a área cadastrada na
Bahia — um retrato de concentração fundiária coerente com a estrutura agrária
brasileira.

**Pergunta 3 — Qual o nível de regularização dos cadastros (situação cadastral)?**
A base está fortemente concentrada em cadastros ativos: **99,42% Ativo (AT)**, 0,36%
Pendente (PE), 0,22% Cancelado (CA) e uma fração residual Suspenso (SU). Isso indica
uma base bem consolidada, com baixíssima incidência de irregularidades — o que se
tornou um desafio relevante para a etapa de modelagem preditiva (ver seção 4).

**Pergunta 4 — Como evoluiu a adesão ao CAR ao longo dos anos?**
A série temporal (2014–2026) mostra um pico claro de adesão em **2017**, com 301.554
novos cadastros naquele ano — mais que o triplo do ano anterior (70.576 em 2016) —
seguido de queda constante até 2022 (68.293 cadastros), com uma leve recuperação em
2023 e 2025. Esse comportamento é coerente com o contexto histórico do CAR no Brasil:
2017 foi o ano de vencimento dos prazos legais originais de adesão obrigatória ao
cadastro (Lei nº 12.651/2012, o "novo Código Florestal"), o que gerou uma onda
concentrada de registros antes do prazo, seguida de uma redução natural conforme a
maior parte dos imóveis do estado já estava cadastrada. O volume mais baixo em 2026
(54.500 cadastros) reflete o ano ainda em curso no momento da coleta dos dados, não
uma queda real de adesão.

**Pergunta 5 — Qual o tamanho médio dos imóveis por tipo de cadastro?**
O tipo de imóvel (`ind_tipo`) tem três categorias na base: **IRU — Imóvel Rural**
(1.312.087 registros, 99,86%, área média de 29,22 ha, módulo fiscal médio 0,57),
**AST — Assentamento** (905 registros, área média de 2.834,72 ha, módulo fiscal
médio 34,08) e **PCT — Povos e Comunidades Tradicionais** (661 registros, área
média de 1.936,60 ha, módulo fiscal médio 17,65). A diferença de escala não é
por acaso: IRU é cadastro individual de propriedade privada, enquanto AST e PCT são
cadastros coletivos de território (assentamentos de reforma agrária e terras de
comunidades tradicionais), por isso a área e o módulo fiscal médios disparam nessas
duas categorias.

## 4. Modelagem preditiva — modelos treinados e comparação de métricas

**Problema:** prever se um cadastro está Ativo ou não (Pendente/Cancelado/Suspenso
agrupados em uma classe "não-Ativo"), a partir de `num_area`, `mod_fiscal`,
`ind_tipo`, `municipio` (top 20 + "Outros") e uma variável derivada,
`dias_desde_atualizacao` (diferença entre `dat_atuali` e `dat_criaca`).

**Desafio de desbalanceamento:** a classe "não-Ativo" representa apenas 0,58% da
base (ver Pergunta 3), o que exigiu ponderação de classes (pesos inversamente
proporcionais à frequência) durante o treino, para evitar que os modelos apenas
aprendessem a prever sempre "Ativo".

**Algoritmos utilizados** (via `pyspark.ml.classification`, módulo nativo de ML
distribuído do Spark):

| Modelo | Família | AUC | Acurácia | Recall (classe não-Ativo) | Precisão (classe não-Ativo) |
|---|---|---|---|---|---|
| Regressão Logística | Linear | 0,696 | 65,3% | 59,4% | 1,0% |
| Random Forest (100 árvores) | Ensemble (árvores) | 0,751 | 82,4% | 51,8% | 1,7% |

O Random Forest teve melhor poder discriminativo geral (AUC mais alto), mas ambos
os modelos apresentam baixíssima precisão para a classe minoritária — resultado
esperado dado o desbalanceamento extremo (1 caso "não-Ativo" para cada ~170 casos
"Ativo"), e não uma falha da modelagem em si. Vale notar que, pelo recall, a
Regressão Logística captura proporcionalmente mais casos reais de "não-Ativo"
(59,4% vs 51,8%) apesar do AUC mais baixo — uma discussão relevante sobre a escolha
de modelo depender do objetivo de negócio (priorizar não deixar passar casos de
risco vs. melhor desempenho médio).

**Importância de variáveis (Random Forest):** `mod_fiscal` (42,1%) e `num_area`
(33,3%) dominam a previsão, respondendo por quase 75% da importância total — o
tamanho do imóvel é, de longe, o fator mais associado à situação cadastral.
`municipio_OUTROS` (11,9%, maior que qualquer município individual) sugere que
propriedades fora dos 20 municípios mais frequentes têm padrão de regularização
diferenciado. `dias_desde_atualizacao` (5,9%) tem peso moderado. `ind_tipo`
praticamente não contribui (< 0,1% em todas as categorias).

## 5. Modelagem descritiva — análise dos clusters

Aplicou-se K-Means sobre as variáveis numéricas `num_area`, `mod_fiscal` e
`dias_desde_atualizacao`, padronizadas com `StandardScaler` (média 0, desvio
padrão 1), já que o algoritmo é sensível a diferenças de escala entre variáveis.

O número de clusters foi escolhido testando k de 2 a 6 e comparando o coeficiente
de silhueta de cada um (métrica que mede coesão interna vs. separação entre
clusters, ao contrário do custo WSSSE, que sempre cai com mais clusters e não serve
sozinho como critério de parada):

| k | Silhueta | Custo (WSSSE) |
|---|---|---|
| 2 | 0,410 | 2.900.825,96 |
| 3 | 0,675 | 1.744.986,28 |
| 4 | 0,388 | 1.596.669,79 |
| 5 | 0,638 | 1.014.841,67 |
| 6 | 0,678 | 784.702,71 |

k=3 e k=6 praticamente empatam como os melhores valores de silhueta; optou-se por
**k=3** pelo melhor equilíbrio entre qualidade do agrupamento e interpretabilidade.

**Perfis identificados (silhueta final = 0,675):**

- **Cluster 1 — Mega-propriedades** (259 imóveis, 0,02% da base): área média de
  16.695 ha e módulo fiscal médio de 256,8 — muito acima do limiar de "grande
  propriedade" (>15 MF), consistente com os assentamentos e territórios coletivos
  identificados na Pergunta 5.
- **Cluster 0 — Pequenas propriedades, cadastro estagnado** (715.338 imóveis,
  54,5%): área média de 18,2 ha, módulo fiscal médio 0,35, sem atualização há em
  média quase 8 anos (2.873 dias).
- **Cluster 2 — Pequenas propriedades, cadastro recente** (598.048 imóveis, 45,5%):
  área média de 41,4 ha, módulo fiscal médio 0,79, atualizado há em média 2,3 anos
  (836 dias).

O achado mais relevante é que o algoritmo não separou os grupos apenas por tamanho
(como fizemos manualmente na classificação de porte por INCRA): o fator que mais
diferencia as pequenas propriedades entre si é o **tempo desde a última
atualização do cadastro** — coerente com o peso que essa mesma variável teve na
modelagem preditiva (seção 4).

## 6. Conclusões e limitações

A base do CAR/SICAR para a Bahia é dominada por pequenas propriedades rurais
individuais (IRU), majoritariamente com cadastro ativo — um retrato coerente com a
estrutura fundiária brasileira. O tamanho do imóvel é o fator mais associado tanto
à situação cadastral (Etapa 4) quanto ao perfil de atualização do cadastro
(Etapa 5), enquanto localização geográfica e tipo de imóvel têm papel secundário.

**Limitações identificadas:**

1. O forte desbalanceamento das classes de status (99,42% Ativo) dificulta prever
   casos raros (pendentes, cancelados, suspensos) com boa precisão — qualquer
   modelo treinado nessa base terá esse limite estrutural, não é uma falha de
   escolha de algoritmo.
2. A base pública não traz contexto administrativo sobre *por que* um cadastro
   ficou tanto tempo sem atualização — a variável `dias_desde_atualizacao` é um
   proxy indireto, não uma explicação causal.
3. O dataset representa um recorte estadual (Bahia); os padrões encontrados não
   podem ser generalizados para outros estados sem nova validação.
4. As datas de criação/atualização do cadastro exigiram tratamento manual de
   formato (texto brasileiro `dd/MM/yyyy`), um lembrete de que dados públicos
   nem sempre seguem convenções internacionais de formatação.
