
progress = 1

# Load pkgs.
if (!require("pacman")) install.packages("pacman")
pacman::p_load(shiny, 
               googleAnalyticsR, 
               googleAuthR, 
               tidyverse, 
               plotly, 
               lubridate, 
               plyr,
               shinythemes,
               rpivotTable,
               config)

ga <- config::get('GA')

options(googleAuthR.client_id = ga$GA_CLIENT_ID,
        googleAuthR.client_secret = ga$GA_CLIENT_SECRET)

# Authenticate and go through the OAuth2 flow first time - 
# specify a filename to save to by passing it in
gar_auth(token = "sc_ga.httr-oauth")

# Retrieve list of GA accounts.
account_list <- ga_account_list()

ui <- fluidPage(theme = shinytheme("lumen"),

          titlePanel("Google Analytics Multi-Channel Attribution"),
      
          fluidRow(
                  column(2, 
                      dateInput("begin_date_select",
                                  "Begin Date:",
                                  min = as.Date('2018-01-01'),
                                  max = Sys.Date() - 1,
                                  value = Sys.Date() - 90)
                  ),
                  
                  column(2,
                      dateInput("end_date_select",
                                  "End Date:",
                                  min = as.Date('2018-01-01'),
                                  max = Sys.Date() - 1,
                                  value = Sys.Date() - 1)
                 ),
                      
                 column(2,
                      selectInput("time_loop", "Time Grouping:",
                                  c("Pick one" = "0",
                                    "by Month" = "2",
                                    "by Week" = "1"))
                ),
                  
                column(3, 
                      selectInput("ga_view_name", "Website View:",
                                   account_list$viewName)
                      
                ),
                      br(),
                      actionButton("goButton", "Calculate")
            ),
            
            fluidRow(
                  
                  column(12, 
                         
                         plotlyOutput("distPlot", width = "100%", height = "100%")
                         
                  )
                  
            ),
            
            fluidRow(
                    
                    column(12,
                           
                           rpivotTableOutput("attribution_pivot", width = "100%")
                           
                    )
                    
            )
            
    )

server <- function(input, output) {
    
        date_end_period <- function(date_select, time_select){
        
                stopifnot(time_select == 1 | time_select == 2)
                stopifnot(is.Date(date_select))
 
                # 1 for weekly / 2 for monthly.
                if (time_select == 1) {
                  
                      # Add the number of days until Saturday.
                      output_date <- as.character(as.Date(date_select + 
                                                      (7 - wday(date_select))))
                  
                } else {

                    if (time_select == 2) {
    
              
                                if(month(date_select) == 12){

                                          month_next <- 1
                                          year_next <- year(date_select) + 1
    
                                } else {
    
                                          month_next <- month(date_select) + 1
                                          year_next <- year(date_select)
    
                                }
    
                                output_date <- as.character(as.Date(paste(year_next, "-",
                                                         month_next, "-01", sep = ""))-1)
              
                    }
              }
        }          
  
      g_mca_data <- reactive({ 
  
          if(input$goButton == 0){return()}
        
          begin_date <- isolate(input$begin_date_select)
          stop_date <- isolate(input$end_date_select)
          time_loop <- isolate(input$time_loop)
          view_name <- isolate(input$ga_view_name)
       
          # ga_id updated to id from view name.
          ga_id <- account_list %>%
                        dplyr::filter(viewName == view_name) %>%
                          dplyr::select(viewId)
        
          # Calculate the end date for the first period - call date_end_period(). 
          end_date <- date_end_period(begin_date, time_loop)
          
          # Calculate number of datasets to retrieve from GA.
          diff <- difftime(stop_date, begin_date, units = 'weeks')
          
          # If it's months, divide by 4.
          if(time_loop == 2){
              
              diff <- diff/4
              
          } 
              
          # How many loops to retrieve datasets. 
          loops <- 1/as.integer(diff)
          
  
          # Set dataframes we're going to use later to NULL.
          ga_channel_attribution <- NULL
          ga_channel_attribution_final <- NULL
          
          # Inserting a progress bar for the creating the plot.
          withProgress(message = 'Retrieving data', style = 'notification', 
                       detail = 'fetching first dataset', value = 0, {
                           
              Sys.sleep(0.25)
          
          # Loop through by week (or month).
          while (end_date <= stop_date) {
              
              # Get the GA source data - this will grab all transactions. 
              ga_conv_campaign <- 
                google_analytics_3(ga_id, 
                                   start = begin_date,
                                   end = end_date,
                                   metrics = c("totalConversions", 
                                               "totalConversionValue"), 
                                   dimensions = c("campaignPath",
                                                  "sourceMediumPath"), 
                                   filters = "mcf:conversionType==Transaction",
                                   samplingLevel = "WALK",
                                   type = "mcf")
              
              
              # Input the campaign path and source path strings.
              merge_source_campaign <- function (campaign, source) {
                
                # The # of campaigns and sources will be identical.
                # Set the # of campaigns from the campaign path string.
                campaign_source_count <- str_count(campaign, " > ") + 1
                
                # Split each of the strings by the campaign/source delimiter.
                campaign_split <- strsplit(campaign, " > ")
                source_split <- strsplit(source, " > ")
                
                # Convert the strings to matrixes, to reference each campaign/source.
                campaign_matrix <- matrix(unlist(campaign_split), ncol=campaign_source_count, byrow = TRUE)
                source_matrix <- matrix(unlist(source_split), ncol=campaign_source_count, byrow = TRUE)  
                
                # Set the loop to the # of campaigns from the campaign path string.
                i <- campaign_source_count
                
                # Use a loop to work through the matrixes and replace 
                # campaigns with the corresponding source/medium, "--", and campaign.
                while (i != 0) {
                  
                  campaign_matrix[1,i] <- 
                    paste(source_matrix[1,i], campaign_matrix[1,i],sep="--")
                  
                  i <- i - 1
                  
                }
  
                # Replacing the ", " with the " > ".
                source_campaign_merge_str <- str_replace_all(toString(campaign_matrix), ", ", " > ")
                
                return(source_campaign_merge_str)
                
              }
              
              # Work through the entire campaign attribution dataframe
              # replacing '(not set)' with the corrolated source/medium.
              for(i in 1:nrow(ga_conv_campaign)){
                
                  campaign <- ga_conv_campaign[i,1]
                  source <- ga_conv_campaign[i,2]
                  
                  # Send the variables to the merge_source_campaign function, 
                  # return the updated campaign path string & replace the current 
                  # campaign path string with the updated one - 
                  # then move to the next row of data.
                  ga_conv_campaign[i,1] <- 
                      merge_source_campaign(as.character(campaign), as.character(source))
                  
              }
              
              # Drop the sourceMediumPath, since we don't need it any more.
              # Create a new dataframe (there's more work ahead...).
              # Remove non-attribution channels from table - 
              # Direct only counts as an initial traffic source in my model.
              ga_conv_source <- 
                ga_conv_campaign %>% 
                      dplyr::mutate(campaign_path = 
                                      lapply(campaignPath, 
                                             gsub, 
                                             pattern = "> \\(direct\\) / \\(none\\): \\(unavailable\\)", 
                                             replacement = ""),
                                    total_conversions = as.integer(totalConversions),
                                    total_conversion_value = as.numeric(totalConversionValue)) %>%
                      tidyr::unnest(campaign_path) %>%
                      dplyr::select(campaign_path,
                                    total_conversions,
                                    total_conversion_value)
              
              
              # Add the number of campaigns.
              ga_conv_source$campaign_source_count <- stringr::str_count(ga_conv_source$campaign_path, ">") + 1
              
              # Calculate the fraction of conversions and conversion value for each
              # campaign. Also, unnest the campaign_path lists - they are all lists.
              ga_conv_source <- 
                ga_conv_source %>% 
                    dplyr::mutate(campaign_source_conversions = total_conversions/campaign_source_count,
                                  campaign_source_conversion_value = total_conversion_value/campaign_source_count) %>%
                    dplyr::select(campaign_path,
                                  campaign_source_count,
                                  campaign_source_conversions,
                                  campaign_source_conversion_value)
              
              # Create attribution channels dataframe.
              ga_attribution_channels <- NULL
              i = 1
              
              # Loop through ga_conv_source, separate all attribution
              # channels with ' > ' as delimter and add each in the 
              # ga_attribution_channels table.
              while (i <= nrow(ga_conv_source)){
                  
                  # If there is more than one source/medium, loop through them
                  # and enter each into ga_attribution_channels
                  # otherwise, move to the next row.
                  if(ga_conv_source[i,'campaign_source_count'] > 1){
                    
                      # Set the temp table to null.
                      ga_attribution_channels_temp <- NULL
                      
                      # Split out the source/medium paths into a character matrix.
                      source_split <- str_split_fixed(ga_conv_source[i,"campaign_path"], 
                                            pattern=fixed(' > '), n=Inf)
                     
                      e <- 1
                      
                      # Loop through the character matrix, 
                      # adding each source/medium and the associated values
                      # to the temp dataframe.
                      while(e <= length(source_split)){
                          
                          # These are multiple source/mediums, 
                          # so each source/medium takes 1/n of the orders & revenue.
                          source_split_fraction <- 
                              data.frame(source_split[1,e], 
                                 (ga_conv_source[i,"campaign_source_conversions"]), 
                                 (ga_conv_source[i,"campaign_source_conversion_value"])) 
                          
                          # Rename source_split_fraction to match with ga_attribution_channels.
                          names(source_split_fraction)<-c("campaign_path",
                                                         "campaign_source_conversions", 
                                                         "campaign_source_conversion_value")
                      
                          ga_attribution_channels_temp <- 
                                      rbind(ga_attribution_channels_temp, source_split_fraction)
                       
                          
                          # Move to the next source/medium/campaign.
                          e <- e + 1
                          
                      }
                      
                  } else{
                    
                    ga_attribution_channels_temp <- 
                              ga_conv_source[i, c('campaign_path',
                                                  'campaign_source_conversions',
                                                  'campaign_source_conversion_value')]
                    
                  }
                  
                  ga_attribution_channels = 
                            rbind(ga_attribution_channels, ga_attribution_channels_temp)
                
                  i <- i + 1
              }
              
              # Group by campaign_path - add them together.
              ga_attribution_channels <- 
                          ga_attribution_channels %>%
                                dplyr::mutate(campaign_path = stringr::str_trim(campaign_path)) %>%
                                dplyr::group_by(campaign_path) %>%
                                dplyr::summarise(campaign_source_conversions = 
                                                        sum(campaign_source_conversions), 
                                                 campaign_source_conversion_value = 
                                                        sum(campaign_source_conversion_value)) 
              
              # Add the dates for this round.
              ga_attribution_channels$begin_date <- begin_date
              ga_attribution_channels$end_date <- end_date
              
              # Add it to the output file before next loop.
              ga_channel_attribution_final <- 
                  rbind(ga_attribution_channels, ga_channel_attribution_final)
              
              # Adjust dates based on weekly or monthly loop.
              begin_date <- as.Date(end_date) + 1
              end_date <- date_end_period(begin_date, time_loop)
              
              # If the start date is greater than the stop date,
              # make the end date greater than the stop date.
              end_date <- ifelse(begin_date > stop_date, begin_date, end_date)
              
              date_select <- paste('Retrieving next data set: ', begin_date, ' to ', end_date, sep = '')
              
              if(progress == 1){
                  incProgress(loops, detail = date_select)
                  Sys.sleep(0.25)
              }
              
          }
                         
          }) # Close of progress bar - all done.
          
          # Split out campaign, source and medium.
          # Round the conversions and conversion values.
          # Add column for month - making sure it's a string
          # that will sort nicely - otherwise plotly will convert it to 
          # a date and leave large gaps between months.
          # Add a column to shorten the medium.
          # Combine source and medium into one column as well.
          ga_channel_attribution_finito <- 
                  ga_channel_attribution_final %>%
                              tidyr::separate("campaign_path", 
                                            c("source", "medium"), sep = " / ") %>%
                              tidyr::separate("medium", 
                                            c("medium", "campaign"), sep = "--") %>%
                              dplyr::mutate(campaign_source_conversions = 
                                                    round(as.integer(campaign_source_conversions), 0),
                                           campaign_source_conversion_value =
                                                    round(campaign_source_conversion_value, 2),
                                           month = paste('M: ', 
                                                         as.character(format(as.Date(begin_date), 
                                                                             '%Y-%m')), sep = ''),
                                           medium_short = substr(medium, 1, 20),
                                           source_medium = paste(medium, source, sep = ' - '))
   
          return(ga_channel_attribution_finito)
        
    }) 
    
    mca_data_out <- function(time_input){
      
          # Retrieve the dataset.
          mca_output <- g_mca_data() 
          
          # Time input is by week.
          if(time_input == "1"){
            
                mca_output <- mca_output %>%
                                dplyr::group_by(source_medium, begin_date) %>%
                                dplyr::summarize(campaign_source_conversions = 
                                                   sum(campaign_source_conversions)) %>%
                                dplyr::ungroup() %>%
                                dplyr::rename(date_select = begin_date) %>%
                                dplyr::arrange(source_medium, date_select)
        
          } 
                    
          # Time input is by month.
          if(time_input == "2"){
            
                  mca_output <- mca_output %>%
                                  dplyr::group_by(source_medium, month) %>%
                                  dplyr::summarize(campaign_source_conversions = 
                                                     sum(campaign_source_conversions)) %>%
                                  dplyr::ungroup() %>%
                                  dplyr::rename(date_select = month) %>%
                                  dplyr::arrange(source_medium, date_select)
            
          }        

          return(mca_output)    
          
    } 
    
    output$distPlot <- renderPlotly({
        
            if(input$goButton == 0){return()}
            if(input$time_loop == "0"){return()}
          
            # time_input <- 1
            time_input <- isolate(input$time_loop)
          
            # Call mca_data.
            mca_data <- mca_data_out(time_input)
    
            # Output will be by week(1) or month(2).
            x_axis_title <- ifelse(time_input == "1", "Week", "Month")
   
            x_format <- list(
              
              title = x_axis_title
              
            )
            
            y_format <- list(
              
              title = 'Conversions'
              
            )
            
            # Draw plot.
            mca_data %>%
                   plot_ly( ., x = ~date_select, 
                            y = ~campaign_source_conversions, 
                            type = 'scatter', 
                            mode = 'lines', 
                            color = ~source_medium) %>%
                            layout(showlegend = TRUE,
                                   autosize = T,
                                   xaxis = x_format,
                                   yaxis = y_format)
        
      })

      output$attribution_pivot <- renderRpivotTable({
      
            if(input$goButton == 0){return()}
            if(input$time_loop == "0"){return()}
                  
            time_input <- isolate(input$time_loop)

            # Call mca_data_out. 
            mca_data <- mca_data_out(time_input)
            
            aggr_name <- 'Integer Sum' 
            
            # Draw the interactive pivot table.
            rpivotTable(data = mca_data, 
                        rows = 'source_medium', 
                        cols = 'date_select', 
                        vals = 'campaign_source_conversions', 
                        aggregatorName = aggr_name)
      
    })

}




