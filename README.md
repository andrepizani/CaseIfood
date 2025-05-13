# 📦 Case Técnico – Data Analytics iFood

Este repositório contém a resolução completa do case técnico proposto pelo iFood, com foco em análise de testes A/B para campanhas promocionais utilizando PySpark no ambiente Databricks.

[Apresentação](https://docs.google.com/presentation/d/1fajbGMI6A6OW5IjKB_XfJxNTUMhoV_u_muCd4DPGclM/edit?slide=id.g357e9fd0664_0_0#slide=id.g357e9fd0664_0_0)



---

## 🚀 Objetivo

Analisar os resultados de uma campanha de cupons para entender seu impacto em:

- Engajamento de usuários
- Volume de pedidos
- Ticket médio
- Retenção e recorrência
- Eficiência financeira (ROI)

---

## 🧪 Metodologia

A análise foi conduzida em **PySpark**, dentro do **Databricks Community Edition**, utilizando transformações em DataFrames e janelas de tempo para cálculo de KPIs por usuário e por grupo de teste.

---

## 📊 KPIs Calculados

- **Usuários únicos**
- **Total de pedidos**
- **Gasto total**
- **Ticket médio por pedido**
- **Ticket médio por usuário**
- **Pedidos por usuário**
- **% de recorrência**
- **Tempo médio entre pedidos**
- **Pedidos no final de semana**
- **ROI estimado com diferentes valores de cupom (R$10, R$15, R$20)**

---

## 🛠️ Tecnologias Utilizadas

- **Plataforma:** [Databricks Community Edition](https://community.cloud.databricks.com/)
- **Linguagem:** PySpark

### 🔧 Principais bibliotecas e funções:
```python
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, avg, when,
    min as _min, max as _max, datediff, lag, dayofweek, round, to_date
)
from pyspark.sql.window import Window
Extras: urllib.request, tarfile, gzip, os, shutil (para manipulação de arquivos)

📈 Resultados
A campanha apresentou resultados positivos, com crescimento expressivo nos principais indicadores. O ROI estimado variou entre 137% e 373% dependendo do valor do cupom simulado.

📌 Próximos passos sugeridos
Rodar novos testes A/B segmentados por perfil

Avaliar o timing da campanha (início/meio/fim do mês)

Integrar campanhas ao CRM com mensagens personalizadas

Medir o impacto no LTV e retenção pós-campanha

