# Load libraries
library(shiny)
library(shinydashboard)
library(DBI)
library(RSQLite)
library(dplyr)
library(ggplot2)
library(DT)
library(lubridate)
library(wordcloud2)

# Connect to SQLite database
con <- dbConnect(SQLite(), "data/news.db")

# Preload dropdown options
categories <- dbGetQuery(con, "SELECT category FROM NewsCategory ORDER BY category")$category
sources <- dbGetQuery(con, "SELECT source FROM NewsSource ORDER BY source")$source

# UI
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
      box(width = 12, title = "Word Cloud of Titles and Descriptions", status= "danger", solidHeader = TRUE,
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

# SERVER
server <- function(input, output, session) {
  filtered_data <- eventReactive(input$apply, {
    start_date <- as.character(input$dates[1])
    end_date   <- as.character(input$dates[2])
    kw <- ifelse(nchar(input$keyword) > 0, paste0("%", input$keyword, "%"), "%")
    
    sql <- "
      SELECT 
          a.id,
          a.title,
          a.description,
          a.published,
          a.url,
          s.source
      FROM NewsArticles a
      LEFT JOIN NewsSource s ON s.id = a.source_id
      WHERE a.published BETWEEN ? AND ?
        AND (? = 'All' OR s.source = ?)
        AND (? = 'All' OR a.id IN (
              SELECT ac.news_id
              FROM NewsArticleCategory ac
              JOIN NewsCategory c ON ac.category_id = c.id
              WHERE c.category = ?
          ))
        AND (a.title LIKE ? OR a.description LIKE ?)
      ORDER BY a.published DESC;
    "
    
    df <- dbGetQuery(con, sql, params = list(
      start_date, end_date,
      input$source, input$source,
      input$category, input$category,
      kw, kw
    ))
    
    df$published <- as.Date(df$published)
    df
  }, ignoreNULL = FALSE)
  
  # returns filtered data if available, else defaults
  get_data <- reactive({
    df <- filtered_data()
    if (is.null(df) || nrow(df) == 0) return(data.frame())
    df
  })
  
  # Articles Over Time Plot
  output$articles_over_time <- renderPlot({
    df <- get_data()
    if (nrow(df) == 0) return(NULL)
    
    df %>%
      count(published) %>%
      ggplot(aes(x = published, y = n)) +
      geom_line(color = "#0d6efd", size = 1.2) +
      geom_point(color = "#198754", size = 2) +
      labs(x = "Date", y = "Count") +
      theme_minimal(base_size = 14) +
      theme(
        plot.title = element_text(face = "bold", size = 16),
        axis.title = element_text(face = "bold")
      )
  })
  
  # Word Cloud (titles + descriptions)
  output$wordcloud <- wordcloud2::renderWordcloud2({
    df <- get_data()
    if (nrow(df) == 0) {
      return(wordcloud2::wordcloud2(data.frame(word = "No Data", n = 1)))
    }
    
    # Combine title + description
    wc_data <- df %>%
      dplyr::select(title, description) %>%
      tidyr::unite("text", title, description, sep = " ", remove = TRUE) %>%
      dplyr::filter(!is.na(text)) %>%
      tidytext::unnest_tokens(word, text) %>%
      anti_join(tidytext::stop_words, by = "word") %>%
      dplyr::filter(!grepl("^[0-9]+$", word)) %>%
      dplyr::count(word, sort = TRUE)
    
    if (nrow(wc_data) == 0) {
      wc_data <- data.frame(word = "No Words", n = 1)
    }
    
    wordcloud2::wordcloud2(wc_data)
  })
  
  # Top Sources Plot
  output$top_sources <- renderPlot({
    df <- get_data()
    if (nrow(df) == 0) return(NULL)
    
    df %>%
      count(source, sort = TRUE) %>%
      head(10) %>%
      ggplot(aes(reorder(source, n), n)) +
      geom_col(fill = "darkgreen") +
      coord_flip() +
      labs(x = "Source", y = "Count") +
      theme_minimal(base_size = 14) +
      theme(
        plot.title = element_text(face = "bold", size = 16),
        axis.title = element_text(face = "bold")
      )
  })
  
  # Top Categories Plot
  output$top_categories <- renderPlot({
    df <- get_data()
    if (nrow(df) == 0) return(NULL)
    
    # Get article-category mapping
    sql <- "
      SELECT ac.news_id, c.category
      FROM NewsArticleCategory ac
      JOIN NewsCategory c ON ac.category_id = c.id
    "
    all_categories <- dbGetQuery(con, sql)
    
    filtered_categories <- all_categories %>%
      filter(news_id %in% df$id) %>%
      count(category, sort = TRUE) %>%
      head(10)
    
    ggplot(filtered_categories, aes(reorder(category, n), n)) +
      geom_col(fill = "steelblue") +
      coord_flip() +
      labs(x = "Category", y = "Count") +
      theme_minimal(base_size = 14) +
      theme(
        plot.title = element_text(face = "bold", size = 16),
        axis.title = element_text(face = "bold")
      )
  })
  
  # Articles Table
  output$table <- renderDT({
    df <- get_data()
    if (nrow(df) == 0) return(NULL)
    
    # Make URL clickable
    df$url <- paste0('<a href="', df$url, '" target="_blank">Link</a>')
    
    df <- df %>% select(-id)
    
    datatable(
      df,
      escape = FALSE,
      options = list(
        pageLength = 10,
        autoWidth = TRUE,
        scrollX = TRUE
      ),
      class = "cell-border stripe hover"
    )
  })
  
}

# Run the app
shinyApp(ui, server)

# Disconnect connection once finished
dbDisconnect(con)
