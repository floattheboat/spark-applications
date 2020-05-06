


#library(sparklyr)
library(dplyr)

#sc <- spark_connect(master = "local", version = "2.3")

scaler <- ft_standard_scaler(
    sc,
    input_col = "features",
    output_col = "features_scaled",
    with_mean = TRUE)

scaler



df <- copy_to(sc, data.frame(value = rnorm(100000))) %>% 
    ft_vector_assembler(input_cols = "value", output_col = "features")

scaler_model <- ml_fit(scaler, df)
scaler_model


scaler_model %>% 
    ml_transform(df) %>%
    glimpse()

# pipeline creation

ml_pipeline(sc) %>% 
    ft_standard_scaler(
        input_col = "features",
        output_col = "features_scaled", 
        with_mean = TRUE)


pipeline <- ml_pipeline(scaler)

pipeline_model <- ml_fit(pipeline, df)
pipeline_model



okc_train <- spark_read_parquet(sc, "data/okc-train.parquet")

okc_train <- okc_train %>% 
    select(not_working, age, sex, drinks, drugs, essay1:essay9, essay_length)


pipeline <- ml_pipeline(sc) %>%
    ft_string_indexer(input_col = "sex", output_col = "sex_indexed") %>%
    ft_string_indexer(input_col = "drinks", output_col = "drinks_indexed") %>%
    ft_string_indexer(input_col = "drugs", output_col = "drugs_indexed") %>%
    ft_one_hot_encoder_estimator(
        input_cols = c("sex_indexed", "drinks_indexed", "drugs_indexed"),
        output_cols = c("sex_encoded", "drinks_encoded", "drugs_encoded")
    ) %>%
    ft_vector_assembler(
        input_cols = c("age", "sex_encoded", "drinks_encoded", 
                       "drugs_encoded", "essay_length"), 
        output_col = "features"
    ) %>%
    ft_standard_scaler(input_col = "features", output_col = "features_scaled", 
                       with_mean = TRUE) %>%
    ml_logistic_regression(features_col = "features_scaled", 
                           label_col = "not_working")



# hyperparameter tuning

cv <- ml_cross_validator(
    sc,
    estimator = pipeline,
    estimator_param_maps = list(
        standard_scaler = list(with_mean = c(TRUE, FALSE)),
        logistic_regression = list(
            elastic_net_param = c(0.25, 0.75),
            reg_param = c(1e-2, 1e-3)
        )
    ),
    evaluator = ml_binary_classification_evaluator(sc, label_col = "not_working"),
    num_folds = 10)

cv


cv_model <- ml_fit(cv, okc_train)


ml_validation_metrics(cv_model) %>%
    arrange(-areaUnderROC)



# interoperability


model_dir <- file.path("spark_model")
ml_save(cv_model$best_model, model_dir, overwrite = TRUE)



list.dirs(model_dir,full.names = FALSE) %>%
    head(10)



spark_read_json(sc, file.path(
    file.path(dir(file.path(model_dir, "stages"),
                  pattern = "1_string_indexer.*",
                  full.names = TRUE), "metadata")
)) %>% 
    glimpse()



spark_read_parquet(sc, file.path(
    file.path(dir(file.path(model_dir, "stages"),
                  pattern = "6_logistic_regression.*",
                  full.names = TRUE), "data")
))


model_reload <- ml_load(sc, model_dir)


ml_stage(model_reload, "logistic_regression")



#### TODO section 5.6 ####

# haven't pasted any code from this section



