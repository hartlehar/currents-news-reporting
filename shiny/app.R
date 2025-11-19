library(shiny)
library(shinydashboard)
library(DBI)
library(RPostgres)
library(dplyr)
library(ggplot2)
library(DT)
library(lubridate)
library(wordcloud2)
library(tidytext)
library(tidyr)

# ---------------------------------------------------------------
#  PostgreSQL Connection
# ---------------------------------------------------------------
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = Sys.getenv("POSTGRES_DB", "airflow"),
  host     = Sys.getenv("POSTGRES_HOST", "postgres"),
  port     = Sys.getenv("POSTGRES_PORT", "5432"),
  user     = Sys.getenv("POSTGRES_USER", "airflow"),
  password = Sys.getenv("POSTGRES_PASSWORD", "airflow")
)

# ---------------------------------------------------------------
#  Preload dropdown options
# ---------------------------------------------------------------
categories <- dbGetQuery(con, "SELECT DISTINCT category FROM NewsCategory ORDER BY category")$category
sources    <- dbGetQuery(con, "SELECT DISTINCT source FROM NewsSource ORDER BY source")$source


# ---------------------------------------------------------------
#  UI
# ---------------------------------------------------------------
ui <- dashboardPage(
  dashboardHeader(title = "World News Dashboard"),
  
  dashboardSidebar(
    width = 250,
    dateRangeInput("dates", "Date Range", start = "2024-01-01", end = Sys.Date()),
    selectInput("category", "Category", choices = c("All", categories)),
    selectInput("source", "Source", choices = c("All", sources)),
    textInput("keyword", "Keyword Search", ""),
    actionButton("apply", "Apply Filters")
  ),
  
  dashboardBody(
    fluidRow(
      box(width = 12, title = "Articles Over Time", status = "primary", solidHeader = TRUE,
          plotOutput("articles_over_time", height = "300px"))
    ),
    fluidRow(
      box(width = 12, title = "Word Cloud of Titles and Descriptions", status = "danger", solidHeader = TRUE,
          wordcloud2Output("wordcloud", height = "400px", width = "100%"))
    ),
    fluidRow(
      box(width = 6, title = "Top Sources", status = "success", solidHeader = TRUE,
          plotOutput("top_sources", height = "300px")),
      box(width = 6, title = "Top Categories", status = "info", solidHeader = TRUE,
          plotOutput("top_categories", height = "300px"))
    ),
    fluidRow(
      box(width = 12, title = "Articles", status = "warning", solidHeader = TRUE,
          DTOutput("table"))
    )
  )
)

# ---------------------------------------------------------------
#  SERVER
# ---------------------------------------------------------------
server <- function(input, output, session) {

  filtered_data <- eventReactive(input$apply, {
    start_date <- as.character(input$dates[1])
    end_date   <- as.character(input$dates[2])
    kw         <- ifelse(nchar(input$keyword) > 0, paste0("%", input$keyword, "%"), "%")
    
    sql <- "
      SELECT 
          a.id,
          a.title,
          a.description,
          a.published,
          a.url,
          s.source
      FROM NewsArticles a
      LEFT JOIN NewsSource s ON s.news_id = a.id
      WHERE a.published BETWEEN $1 AND $2
        AND ($3 = 'All' OR s.source = $3)
        AND ($4 = 'All' OR a.id IN (
              SELECT news_id
              FROM NewsCategory
              WHERE category = $4
          ))
        AND (a.title ILIKE $5 OR a.description ILIKE $5)
      ORDER BY a.published DESC;
    "
    
    df <- dbGetQuery(con, sql, params = list(
      start_date, end_date, input$source, input$category, kw
    ))
    
    df$published <- as.Date(df$published)
    df
  }, ignoreNULL = FALSE)

  
  get_data <- reactive({
    df <- filtered_data()
    if (is.null(df) || nrow(df) == 0) return(data.frame())
    df
  })


  # -----------------------------------------------------------
  #  Articles over time
  # -----------------------------------------------------------
  output$articles_over_time <- renderPlot({
    df <- get_data()
    if (nrow(df) == 0) return(NULL)
    
    df %>% 
      count(published) %>% 
      ggplot(aes(x = published, y = n)) +
      geom_line(color = "#0d6efd", size = 1.3) +
      geom_point(color = "#198754", size = 2.5) +
      labs(x = "Date", y = "Count") +
      theme_minimal(base_size = 14)
  })
  

  # -----------------------------------------------------------
  #  Word Cloud
  # -----------------------------------------------------------
  output$wordcloud <- wordcloud2::renderWordcloud2({
    df <- get_data()
    if (nrow(df) == 0)
      return(wordcloud2(data.frame(word="No Data", n=1)))
    
    wc_data <- df %>%
      select(title, description) %>%
      unite("text", title, description, sep=" ") %>%
      filter(!is.na(text)) %>%
      unnest_tokens(word, text) %>%
      anti_join(stop_words, by="word") %>%
      filter(!grepl("^[0-9]+$", word)) %>%
      count(word, sort=TRUE)
    
    wordcloud2(wc_data)
  })


  # -----------------------------------------------------------
  #  Top Sources
  # -----------------------------------------------------------
  output$top_sources <- renderPlot({
    df <- get_data()
    if (nrow(df) == 0) return(NULL)
    
    df %>% 
      count(source, sort = TRUE) %>% head(10) %>% 
      ggplot(aes(reorder(source, n), n)) +
      geom_col(fill="darkgreen") +
      coord_flip() +
      labs(x="Source", y="Count") +
      theme_minimal(base_size = 14)
  })


  # -----------------------------------------------------------
  #  Top Categories
  # -----------------------------------------------------------
  output$top_categories <- renderPlot({
    df <- get_data()
    if (nrow(df) == 0) return(NULL)
    
    sql <- "SELECT news_id, category FROM NewsCategory"
    all_cat <- dbGetQuery(con, sql)
    
    filtered_cat <- all_cat %>%
      filter(news_id %in% df$id) %>%
      count(category, sort=TRUE) %>%
      head(10)
    
    ggplot(filtered_cat, aes(reorder(category, n), n)) +
      geom_col(fill="steelblue") +
      coord_flip() +
      labs(x="Category", y="Count") +
      theme_minimal(base_size=14)
  })


  # -----------------------------------------------------------
  #  Articles Table
  # -----------------------------------------------------------
  output$table <- renderDT({
    df <- get_data()
    if (nrow(df) == 0) return(NULL)

    df$url <- paste0('<a href="', df$url, '" target="_blank">Link</a>')

    df <- df %>% select(-id)

    datatable(
      df,
      escape = FALSE,
      options = list(pageLength = 10, scrollX = TRUE),
      class = "cell-border stripe hover"
    )
  })

}

# ---------------------------------------------------------------
#  Run
# ---------------------------------------------------------------
shinyApp(ui, server)