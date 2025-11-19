FROM rocker/shiny:4.3.1

# Create app directory
RUN mkdir /R_app

# Set working directory
WORKDIR /R_app

COPY src/R_app/ .
COPY data/ /R_app/data

# Install R packages
RUN Rscript install_packages.R

# Expose port
EXPOSE 3838

# Run Shiny app
CMD ["R", "-e", "shiny::runApp('/R_app', host='0.0.0.0', port=3838)"]
