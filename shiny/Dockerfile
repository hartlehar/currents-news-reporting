FROM rocker/shiny:4.3.1

# Create app directory
RUN mkdir /shiny

# Set working directory
WORKDIR /shiny

# Copy Shiny app files
COPY ./shiny/ .

# Copy data into the container
COPY ./data/ ./data/

# Install R packages
RUN Rscript install_packages.R

# Expose port
EXPOSE 3838

# Run Shiny app
CMD ["R", "-e", "shiny::runApp('/shiny', host='0.0.0.0', port=3838)"]
