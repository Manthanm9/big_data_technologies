from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, MinMaxScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("IrisClassification").getOrCreate()

    # Step 1: Read the data from a CSV file

    df = spark.read.csv('iris.csv', header=True, inferSchema=True)

    # Step 2: Prepare the data

    # Assemble the feature columns into a single vector column 'features'
    feature_cols = ['sepallength', 'sepalwidth', 'petallength', 'petalwidth']
    assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    df = assembler.transform(df)

    # Convert the 'class' column (species) to a numerical label column 'label'
    indexer = StringIndexer(inputCol='class', outputCol='label')
    df = indexer.fit(df).transform(df)

    # Step 3: Scale the features
    scaler = MinMaxScaler(inputCol='features', outputCol='scaled_features')
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)

    # Step 4: Build a machine learning pipeline with RandomForestClassifier
    rf = RandomForestClassifier(labelCol='label', featuresCol='scaled_features', numTrees=100)

    # Step 5: Split the data into training and testing sets
    (training_data, testing_data) = df.randomSplit([0.8, 0.2], seed=42)

    # Step 6: Train the model
    model = rf.fit(training_data)

    # Step 7: Make predictions on the test data
    predictions = model.transform(testing_data)

    # Step 8: Evaluate the model
    evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
    accuracy = evaluator.evaluate(predictions)
    print(f"Accuracy: {accuracy}")

    # Stop the SparkSession
    spark.stop()

