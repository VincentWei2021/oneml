from pyspark.sql import SQLContext,SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml.feature import Normalizer
from pyspark.ml.linalg import Vectors

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression, OneVsRest


#CONSTANTS
DataLOADER = "dataLoader"
Sampler = "sampling"
Option = "option"
Steps = "steps"
Path = "path"
Format = "format"
Sampling_Stratified = "stratified"
Preprocessing = "preprocessing"
Variables = "variables"
LabelCol = "labelCol"
TextCol = "textCol"
categoricalCol = "categoricalCol"
numerical = "numericalCol"
normalization = "normalization"
MLAlgorithm = "mlAlgorithm"
libsvm = "libsvm"
sampling_ratio_arr = "ratio"
seed_val = 0.1
featureGeneration = "featureGeneration"
from pyspark.ml.feature import Bucketizer

spark = SparkSession.builder.getOrCreate()

def pipelineRun(config:dict):
	#print(config)

	# Data Loader
	if DataLOADER in config[Steps]:
		print("\n\n\n")
		print("------DATA LOADER ---------")

		data_format:str = config[DataLOADER][Format]
		data_path:str = config[DataLOADER][Path]
		dataLoader_df = spark.read.options(header=True).csv(data_path)

		#SparkSession.builder \
		#	.getOrCreate() \
		#	.read \
		#		.format(data_format) \
		#		.read \
		#		.options(header=True) \
		#	.csv(data_path) \


		print("Data Loaded")
		print("Number of Rows:",dataLoader_df.count())
		rowCount = dataLoader_df.count()

		print("columns",dataLoader_df.columns)
		required_Cols = config[Variables]
		reqd_cols_arr = []
		#for var in required_Cols:
		reqd_cols_arr.append(config[Variables][LabelCol])

		textCol = config[Variables][TextCol]
		if textCol:
			reqd_cols_arr += textCol

		numericalCol = config[Variables][numerical]
		if numericalCol:
			reqd_cols_arr += numericalCol

		categoricalcol = config[Variables][categoricalCol]
		if categoricalcol:
			reqd_cols_arr += categoricalcol

		print("selected Columns:", reqd_cols_arr)
		print("\n\n\n")

		dataLoader_df = dataLoader_df.select(*reqd_cols_arr)

	# Sampler
	if Sampler in config[Steps]:
		
		print("---------SAMPLING-------")
		sampling_step_dict = config[Sampler]["sampling_step"][Option]
		if Sampling_Stratified == sampling_step_dict:
			pass
		else:
			# Limited size
			hard_size_limit = config[Sampler]["sampling_step"]["limit_size"]
			
			#dataLoader_df = dataLoader_df.take(hard_size_limit)
			dataLoader_df = dataLoader_df.sample(seed_val,hard_size_limit)
			print("limited size",hard_size_limit)
		# Delay for viewing the spark ui
		print("\n\n\n")

	# Preprocessing
		print("-------PREPROCESSING-------")
		preprocessing_col = config[Preprocessing][Steps][normalization]

		print("normalizing column",preprocessing_col)

		normalizer = Normalizer(inputCol = preprocessing_col, outputCol = preprocessing_col + "_", p = 1.0)
		#dataLoader_df = normalizer.transform(dataLoader_df)
		print("\n\n\n")

	# Feature Generation
		print("------FEATURE GENERATION--------")
		print("\n\n\n")

		featureGenerationChoice = config[featureGeneration]
		if len(featureGenerationChoice) > 0:
			steps = featureGenerationChoice.get("steps")
			for step in steps:
				col = step,get("col")
				feat_gen = step.get(col)
				if feat_gen is "bucketizer":
					Bucketizer = Bucketizer(splits = 100, inputCol = col, outputCol = col+"_")
					dataLoader_df = bucketizer.transform(dataLoader_df)
				if feat_gen is "ngram":
					ngram = NGram(n=5, inputCol=col, outputCol =col+"_")
					dataLoader_df = ngram.transform(dataLoader_df)



	# ML Algorithm
		print("----------- ML ALGORITHM --------")
		print("\n")

		ml_algorithm = config[MLAlgorithm]
		print("ML Algorithm specified", ml_algorithm)

		# generate the train/test split.
		sampling_ratio = config[Sampler][sampling_ratio_arr]
		(train, test) = dataLoader_df.randomSplit(sampling_ratio,seed=0.005)

		
		if ml_algorithm == "Logistic_Regression":
		# instantiate the base classifier.
			lr = LogisticRegression(maxIter=10, tol=1E-6, fitIntercept=True)

			# instantiate the One Vs Rest Classifier.
			ovr = OneVsRest(classifier=lr)

			# train the multiclass model.
			ovrModel = ovr.fit(train)

			# score the model on test data.
			predictions = ovrModel.transform(test)

		else:
			print("Support Vector Machine Algorithm used\n")
			lsvc = LinearSVC(maxIter=10, regParam=0.1)

			# Fit the model
			lsvcModel = lsvc.fit(training)
			predictions = lsvcModel.transform(test)

		# obtain evaluator.
		evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

		# compute the classification error on test data.
		accuracy = evaluator.evaluate(predictions)
		print("Test Error = %g" % (1.0 - accuracy))
		print("accuracy:", accuracy)
		print("\n\n\n")

		"""
		train,test = dataLoader_df.randomSplit([0.7,0.3])
		lr = LogisticRegression(maxIter= 10, fitIntercept = True)
		ovr = OneVsRest(classifier = lr)
		ovrModel = ovr.fit(train)
		predictions = ovrModel.transform(test)

		evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
		accuracy = evaluator.evaluate(predictions)
		print("Test Error = %g" % (1.0 - accuracy))


		"""



