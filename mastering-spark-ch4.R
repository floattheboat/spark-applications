
#### download data ####


# download.file(
#     "https://github.com/r-spark/okcupid/raw/master/profiles.csv.zip",
#     "okcupid.zip"
# )
# 
# 
# unzip("okcupid.zip", exdir = "data")
# unlink("okcupid.zip")




# sampling (not recommended since the dataset is small)

# profiles <- read.csv("data/profiles.csv")
# write.csv(dplyr::sample_n(profiles, 10^3),
#           "data/profiles.csv", row.names = FALSE)


#### libraries ####

library(sparklyr)
library(ggplot2)
library(dbplot)
library(dplyr)
library(ggmosaic)
library(forcats)
library(tidyr)


#sc <- spark_connect(master = "local")


#### read data ####

okc <- spark_read_csv(
    sc,
    "data/profiles.csv",
    escape = "\"",
    memory = FALSE,
    options = list(multiline = TRUE)
) %>% 
    mutate(
        height = as.numeric(height),
        income = ifelse(income == "-1", NA, as.numeric(income))
    ) %>% 
    mutate(sex = ifelse(is.na(sex), "missing", sex)) %>% 
    mutate(drinks = ifelse(is.na(drinks), "missing", drinks)) %>% 
    mutate(drugs = ifelse(is.na(drugs), "missing", drugs)) %>% 
    mutate(job = ifelse(is.na(job), "missing", job)) %>% 
    mutate(religion = ifelse(is.na(religion), "missing", religion)) %>% 
    mutate(ethnicity = ifelse(is.na(ethnicity), "missing", ethnicity))


glimpse(okc)




# add response variable

okc <- okc %>% 
    mutate(
        not_working = ifelse(job %in% c("student", "unemployed", "retired"), 1, 0)
    )

okc %>% 
    group_by(not_working) %>% 
    tally()


# split data into train and test so we don't bias the model-building process



data_splits <- sdf_random_split(okc, training = 0.8, testing = 0.2, seed = 42)

okc_train <- data_splits$training

okc_test <- data_splits$testing


#### explore ####

okc_train %>% 
    group_by(not_working) %>% 
    tally() %>% 
    mutate(frac = n / sum(n))


sdf_describe(okc_train, cols = c("age", "income"))


dbplot_histogram(okc_train, income)


prop_data <- okc_train %>%
    mutate(religion = regexp_extract(religion, "^\\\\w+", 0)) %>% 
    group_by(religion, not_working) %>%
    tally() %>%
    group_by(religion) %>%
    summarize(
        count = sum(n),
        prop = sum(not_working * n) / sum(n)
    ) %>%
    mutate(se = sqrt(prop * (1 - prop) / count)) %>%
    collect()

prop_data



prop_data %>%
    ggplot(aes(x = religion, y = prop)) + geom_point(size = 2) +
    geom_errorbar(aes(ymin = prop - 1.96 * se, ymax = prop + 1.96 * se),
                  width = .1) +
    geom_hline(yintercept = sum(prop_data$prop * prop_data$count) /
                   sum(prop_data$count))


# contingency table via crosstab

contingency_tbl <- okc_train %>% 
    sdf_crosstab("drinks", "drugs") %>%
    collect()

contingency_tbl


# mosaic plot

contingency_tbl %>%
    rename(drinks = drinks_drugs) %>%
    gather("drugs", "count", missing:sometimes) %>%
    mutate(
        drinks = as_factor(drinks) %>% 
            fct_relevel("missing", "not at all", "rarely", "socially", 
                        "very often", "desperately"),
        drugs = as_factor(drugs) %>%
            fct_relevel("missing", "never", "sometimes", "often")
    ) %>%
    ggplot() +
    geom_mosaic(aes(x = product(drinks, drugs), fill = drinks, 
                    weight = count))



# correspondence analysis

dd_obj <- contingency_tbl %>% 
    tibble::column_to_rownames(var = "drinks_drugs") %>%
    FactoMineR::CA(graph = FALSE)


dd_drugs <-
    dd_obj$row$coord %>%
    as.data.frame() %>%
    mutate(
        label = gsub("_", " ", rownames(dd_obj$row$coord)),
        Variable = "Drugs"
    )

dd_drinks <-
    dd_obj$col$coord %>%
    as.data.frame() %>%
    mutate(
        label = gsub("_", " ", rownames(dd_obj$col$coord)),
        Variable = "Alcohol"
    )

ca_coord <- rbind(dd_drugs, dd_drinks)

ggplot(ca_coord, aes(x = `Dim 1`, y = `Dim 2`, 
                     col = Variable)) +
    geom_vline(xintercept = 0) +
    geom_hline(yintercept = 0) +
    geom_text(aes(label = label)) +
    coord_equal()


#### feature engineering ####

scale_values <- okc_train %>%
    summarize(
        mean_age = mean(age),
        sd_age = sd(age)
    ) %>%
    collect()

scale_values


okc_train <- okc_train %>%
    mutate(scaled_age = (age - !!scale_values$mean_age) /
               !!scale_values$sd_age)


dbplot_histogram(okc_train, scaled_age)


okc_train %>%
    group_by(ethnicity) %>%
    tally()


ethnicities <- c("asian", "middle eastern", "black", "native american", "indian", 
                 "pacific islander", "hispanic / latin", "white", "other")
ethnicity_vars <- ethnicities %>% 
    purrr::map(~ expr(ifelse(like(ethnicity, !!.x), 1, 0))) %>%
    purrr::set_names(paste0("ethnicity_", gsub("\\s|/", "", ethnicities)))
okc_train <- mutate(okc_train, !!!ethnicity_vars)
okc_train %>% 
    select(starts_with("ethnicity_")) %>%
    glimpse()


okc_train <- okc_train %>%
    mutate(
        essay_length = char_length(paste(!!!syms(paste0("essay", 0:9))))
    ) %>% compute()


dbplot_histogram(okc_train, essay_length, bins = 100)


spark_write_parquet(okc_train, "data/okc-train.parquet")



#### supervised learning ####


# create folds

vfolds <- sdf_random_split(
    okc_train,
    weights = purrr::set_names(rep(0.1, 10), paste0("fold", 1:10)),
    seed = 42
)


# create first analysis/assessment split

analysis_set <- do.call(rbind, vfolds[2:10])
assessment_set <- vfolds[[1]]


make_scale_age <- function(analysis_data) {
    scale_values <- analysis_data %>%
        summarize(
            mean_age = mean(age),
            sd_age = sd(age)
        ) %>%
        collect()
    
    function(data) {
        data %>%
            mutate(scaled_age = (age - !!scale_values$mean_age) / !!scale_values$sd_age)
    }
}

scale_age <- make_scale_age(analysis_set)
train_set <- scale_age(analysis_set)
validation_set <- scale_age(assessment_set)


# test regression on a single fold

lr <- ml_logistic_regression(
    analysis_set, not_working ~ scaled_age + sex + drinks + drugs + essay_length
)
lr


validation_summary <- ml_evaluate(lr, assessment_set)
validation_summary


roc <- validation_summary$roc() %>%
    collect()

#roc1 <- validation_summary$roc()

ggplot(roc, aes(x = FPR, y = TPR)) +
    geom_line() + geom_abline(lty = "dashed")

validation_summary$area_under_roc()


# all 10 folds now

cv_results <- purrr::map_df(1:10, function(v) {
    analysis_set <- do.call(rbind, vfolds[setdiff(1:10, v)]) %>% compute()
    assessment_set <- vfolds[[v]]
    
    scale_age <- make_scale_age(analysis_set)
    train_set <- scale_age(analysis_set)
    validation_set <- scale_age(assessment_set)
    
    model <- ml_logistic_regression(
        analysis_set, not_working ~ scaled_age + sex + drinks + drugs + essay_length
    )
    s <- ml_evaluate(model, assessment_set)
    roc_df <- s$roc() %>% 
        collect()
    auc <- s$area_under_roc()
    
    tibble(
        Resample = paste0("Fold", stringr::str_pad(v, width = 2, pad = "0")),
        roc_df = list(roc_df),
        auc = auc
    )
})


# plot all curves

unnest(cv_results, roc_df) %>%
    ggplot(aes(x = FPR, y = TPR, color = Resample)) +
    geom_line() + geom_abline(lty = "dashed")


mean(cv_results$auc)



glr <- ml_generalized_linear_regression(
    analysis_set, 
    not_working ~ scaled_age + sex + drinks + drugs, 
    family = "binomial"
)

tidy_glr <- tidy(glr)


tidy_glr %>%
    ggplot(aes(x = term, y = estimate)) +
    geom_point() +
    geom_errorbar(
        aes(ymin = estimate - 1.96 * std.error, 
            ymax = estimate + 1.96 * std.error, width = .1)
    ) +
    coord_flip() +
    geom_hline(yintercept = 0, linetype = "dashed")



# neural net classifier

nn <- ml_multilayer_perceptron_classifier(
    analysis_set,
    not_working ~ scaled_age + sex + drinks + drugs + essay_length, 
    layers = c(12, 64, 64, 2)
)


predictions <- ml_predict(nn, assessment_set)

ml_binary_classification_evaluator(predictions)


#### section 4.5 - unsupervised learning ####


essay_cols <- paste0("essay", 0:9)
essays <- okc %>%
    select(!!essay_cols)
essays %>% 
    glimpse()


essays <- essays %>%
    # Replace `missing` with empty string.
    mutate_all(list(~ ifelse(. == "missing", "", .))) %>%
    # Concatenate the columns.
    mutate(essay = paste(!!!syms(essay_cols))) %>%
    # Remove miscellaneous characters and HTML tags
    mutate(words = regexp_replace(essay, "\\n|&nbsp;|<[^>]*>|[^A-Za-z|']", " "))


# topic modeling

stop_words <- ml_default_stop_words(sc) %>%
    c(
        "like", "love", "good", "music", "friends", "people", "life",
        "time", "things", "food", "really", "also", "movies"
    )

lda_model <-  ml_lda(essays, ~ words, k = 6, max_iter = 1, min_token_length = 4, 
                     stop_words = stop_words, min_df = 5)


betas <- tidy(lda_model)
betas



betas %>%
    group_by(topic) %>%
    top_n(10, beta) %>%
    ungroup() %>%
    arrange(topic, -beta) %>%
    mutate(term = reorder(term, beta)) %>%
    ggplot(aes(term, beta, fill = factor(topic))) +
    geom_col(show.legend = FALSE) +
    facet_wrap(~ topic, scales = "free") +
    coord_flip()






















