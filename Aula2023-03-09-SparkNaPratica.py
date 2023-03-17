#Arquivo modificado
# Databricks notebook source
# DBTITLE 1,Execução única
# Rodar este notebook apenas uma vez, a tabela ficará disponível pra sempre na sua conta do Databricks Community

spark.conf.set("fs.azure.account.key.databoxhml.blob.core.windows.net","8woRC1uvbZahU9tE/LIzosTmZyfGapMQFBRk8n8sXGLRlm6tUFFq2eHAVOadbAP2YEYRv1a9Nwld+AStdxm2Ww==")

dfPokemon = spark.read.format('delta').load('wasbs://databox@databoxhml.blob.core.windows.net/pokemons')
dfTypes = spark.read.format('delta').load('wasbs://databox@databoxhml.blob.core.windows.net/types')

dfPokemon.write.format('delta').option("mergeSchema", "true").mode('overwrite').saveAsTable('pokemon', path='/FileStore/tables/pokemon')
dfTypes.write.format('delta').option("mergeSchema", "true").mode('overwrite').saveAsTable('types', path='/FileStore/tables/types')

# COMMAND ----------

# DBTITLE 1,Carga dos dataframes iniciais
import pyspark.sql.functions as fn

dfPokemon = spark.read.format('delta').load('/FileStore/tables/pokemon').cache()
dfTypes = spark.read.format('delta').load('/FileStore/tables/types').cache()

# COMMAND ----------

dfPokemon.display()

# COMMAND ----------

dfTypes.display()

# COMMAND ----------

# joins
# explode
# collect_list
# case->when

# COMMAND ----------

# DBTITLE 1,Join
#caso as colunas tenham o mesmo nome
#dfPokemon.join(dfTypes, ['nome_coluna'])

#colunas diferentes
dfPokemon.join(dfTypes, dfPokemon.type == dfTypes.name, how ='inner').display()

# COMMAND ----------

# DBTITLE 1,Explode
#explode: ignora os valores vazios
#explode_outer: retorna as linhas com valores vazios
dfPokemon.select('nome', fn.explode_outer('formas')).display()

# COMMAND ----------

# DBTITLE 1,Collect List
#é o inverso do explode
#collect set remove os duplicados
dfPokemon.groupBy('altura').agg(fn.collect_list('nome').alias('lista_poke')).display()

# COMMAND ----------

# DBTITLE 1,Case When
#lit serve para gravar o valor
(dfPokemon
 .withColumn('classe_altura', 
             fn.when(fn.col('altura') < 20, fn.lit('baixinho'))
            .otherwise(fn.lit('altinhos')))            
).display()

# COMMAND ----------

# DBTITLE 1,Pergunta 1
# mostre na tela 10 pokemons e sua lista de movimentos
dfPokemon.join(dfTypes, dfPokemon.type == dfTypes.name, how ='inner').limit(10).display()

# COMMAND ----------

# DBTITLE 1,Pergunta 2
# mostre um explode SOMENTE dos pokemons que possuem mais de uma forma
(
  dfPokemon
  .filter(fn.size('formas') > 1)
  .select('nome', fn.explode_outer('formas'))
).display()

# COMMAND ----------

# DBTITLE 1,Pergunta 3
# mostre o nome dos pokemons e uma lista simples de seus movimentos
(
  dfPokemon
  .join(dfTypes, dfPokemon.type == dfTypes.name, how ='inner')
  .select('nome', fn.explode_outer('moves').alias('moves'))
  .select('nome', fn.col('moves.name').alias('move_name'))
  .groupBy('nome').agg(fn.collect_list('move_name'))
).display()

#resposta 2
#(
#  dfPokemon
#  .join(dfTypes, dfPokemon.type == dfTypes.name, how ='inner')
#  .groupBy('nome').agg(fn.collect_list('moves.name')[0])
#).display()

# COMMAND ----------

# DBTITLE 1,Pergunta 4
# dos pokemons do tipo 'rock', quantos movimentos possuem os pokemons mais pesados?
pesoMax = dfPokemon.filter(fn.col('type') == 'rock').select(fn.max('peso')).first()[0]

(
  dfPokemon
  .filter((fn.col('type') == 'rock') & (fn.col('peso') == pesoMax))
  .join(dfTypes, dfPokemon.type == dfTypes.name, how ='inner')
  .withColumn('qtdMoves', fn.size('moves'))
  .select('nome','peso','type','qtdMoves')
).display()


# COMMAND ----------

# DBTITLE 1,Pergunta 5
# considerando que pokemons leves estão abaixo de 1000kg, mostre quantos pokemos leves e pesados existem em cada categoria de peso
(dfPokemon
 .withColumn('classe_peso', 
             fn.when(fn.col('peso') < 1000, fn.lit('leve'))
            .otherwise(fn.lit('pesado')))   
            .groupBy('classe_peso').count()
).display()

# COMMAND ----------

# DBTITLE 1,Desafio
#pokemon mais forte (mais experiencia) de cada tipo
from pyspark.sql.window import Window

#cria uma janela de dados baseado em uma coluna
wspec = Window.partitionBy('type').orderBy(fn.col('experiencia').desc())

#row_number: cria o número de cada linha

(
  dfPokemon
  .filter(fn.col('experiencia').isNotNull())
  .withColumn()
).display()
