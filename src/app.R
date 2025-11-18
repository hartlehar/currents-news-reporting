# Load libraries
library(shiny)
library(DBI)
library(RSQLite)
library(dplyr)
library(ggplot2)
library(DT)
library(lubridate)

# Connect to SQLite database
con <- dbConnect(SQLite(), "data/news.db")

# Preload dropdown options
categories <- dbGetQuery(con, "SELECT category FROM NewsCategory ORDER BY category")$category
sources <- dbGetQuery(con, "SELECT source FROM NewsSource ORDER BY source")$source

# UI
ui <- fluidPage(
  titlePanel("World News Dashboard"),
  
  sidebarLayout(
    sidebarPanel(
      dateRangeInput("dates", "Date Range", 
                     start = "2024-01-01",
                     end = Sys.Date()),
      selectInput("category", "Category", choices = c("All", categories)),
      selectInput("source", "Source", choices = c("All", sources)),
      textInput("keyword", "Keyword Search", ""),
      actionButton("apply", "Apply Filters")
    ),
    
    mainPanel(
      tabsetPanel(
        tabPanel("Summary",
                 plotOutput("articles_over_time"),
                 plotOutput("top_sources"),
                 plotOutput("top_categories")
        ),
        tabPanel("Articles",
                 DTOutput("table")
        )
      )
    )
  )
)

# SERVER
server <- function(input, output, session) {
  
  # Reactive filtered data
  filtered_data <- eventReactive(input$apply, {
    
    # Convert dates to character for SQLite
    start_date <- as.character(input$dates[1])
    end_date   <- as.character(input$dates[2])
    
    # Handle empty keyword
    kw <- ifelse(nchar(input$keyword) > 0,
                 paste0("%", input$keyword, "%"),
                 "%")
    
    sql <- "
      SELECT 
          a.id,
          a.title,
          a.description,
          a.published,
          s.source
      FROM NewsArticles a
      LEFT JOIN NewsArticleSource asrc ON a.id = asrc.news_id
      LEFT JOIN NewsSource s ON s.id = asrc.source_id
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
    
    # Convert published to Date
    df$published <- as.Date(df$published)
    
    df
  })
  
  # Articles Over Time Plot
  output$articles_over_time <- renderPlot({
    df <- filtered_data()
    if (is.null(df) || nrow(df) == 0) return(NULL)
    
    df %>%
      count(published) %>%
      ggplot(aes(x = published, y = n)) +
      geom_line(color = "steelblue") +
      geom_point(color = "darkblue") +
      labs(title = "Articles Over Time", x = "Date", y = "Count") +
      theme_minimal()
  })
  
  # Top Sources Plot
  output$top_sources <- renderPlot({
    df <- filtered_data()
    if (is.null(df) || nrow(df) == 0) return(NULL)
    
    df %>%
      count(source, sort = TRUE) %>%
      head(10) %>%
      ggplot(aes(reorder(source, n), n)) +
      geom_col(fill = "darkgreen") +
      coord_flip() +
      labs(title = "Top Sources", x = "Source", y = "Count") +
      theme_minimal()
  })
  
  # Top Categories Plot
  output$top_categories <- renderPlot({
    sql <- "
      SELECT c.category AS category, COUNT(*) AS n
      FROM NewsArticleCategory ac
      JOIN NewsCategory c ON ac.category_id = c.id
      GROUP BY c.category
      ORDER BY n DESC
      LIMIT 10;
    "
    df <- dbGetQuery(con, sql)
    
    ggplot(df, aes(reorder(category, n), n)) +
      geom_col(fill = "steelblue") +
      coord_flip() +
      labs(title = "Top Categories", x = "Category", y = "Frequency") +
      theme_minimal()
  })
  
  # Articles DataTable
  output$table <- renderDT({
    filtered_data()
  })
  
}

# Run the app
shinyApp(ui, server)
