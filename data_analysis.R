library(dplyr)
library(ggplot2)
library(RSQLite)
library(tidyr)
library(lubridate)

# Set up the connection to the SQLite database
connection_new <- RSQLite::dbConnect(RSQLite::SQLite(), "new_database.db")

# Check if the connection is successful
if (dbIsValid(connection_new)) {
  cat("Connection to the database established successfully.\n")
} else {
  stop("Failed to establish connection to the database.")
}

# Load the data from CSV files into data frames
advertisement <- read.csv("shoes_data/advertisement.csv")
category <- read.csv("shoes_data/category.csv")
customer <- read.csv("shoes_data/customer.csv")
discount <- read.csv("shoes_data/discount.csv")
inventory <- read.csv("shoes_data/inventory.csv")
orders <- read.csv("shoes_data/orders.csv")
rating <- read.csv("shoes_data/rating.csv")
shipping <- read.csv("shoes_data/shipping.csv")
transactions <- read.csv("shoes_data/transactions.csv")


dbWriteTable(connection_new, "advertisement", advertisement, row.names=FALSE, overwrite = TRUE)
dbWriteTable(connection_new, "category", category, row.names=FALSE, overwrite = TRUE)
dbWriteTable(connection_new, "customer", customer, row.names=FALSE, overwrite = TRUE)
dbWriteTable(connection_new, "discount", discount, row.names=FALSE, overwrite = TRUE)
dbWriteTable(connection_new, "inventory", inventory, row.names=FALSE, overwrite = TRUE)
dbWriteTable(connection_new, "orders", orders, row.names=FALSE, overwrite = TRUE)
dbWriteTable(connection_new, "rating", rating, row.names=FALSE, overwrite = TRUE)
dbWriteTable(connection_new, "shipping", shipping, row.names=FALSE, overwrite = TRUE)
dbWriteTable(connection_new, "transactions", transactions, row.names=FALSE, overwrite = TRUE)

# Top rated products 
top_rated_prod <- RSQLite::dbGetQuery(connection_new, "SELECT p.product_id,
                ROUND(AVG(r.rating), 1) AS product_rating
                FROM inventory p
                INNER JOIN rating r ON p.product_id = r.product
                GROUP BY p.product_id
                ORDER BY product_rating DESC
                LIMIT 10;")

# Top categories 
top_cat <- RSQLite::dbGetQuery(connection_new, "
  SELECT c.category_id,
         c.category_name,
         SUM(r.rating) AS product_rating
  FROM (
    SELECT i.product_id, i.category AS category_id, c.category_name
    FROM inventory AS i
    INNER JOIN category AS c ON i.category = c.category_id
  ) AS c
  INNER JOIN rating AS r ON c.product_id = r.product
  GROUP BY c.category_id, c.category_name
  ORDER BY product_rating DESC
  LIMIT 10;
")

# Top expensive products
top_expensive_products <- RSQLite::dbGetQuery(connection_new, "
  SELECT product_id,
         MAX(price) AS max_price
  FROM inventory
  GROUP BY product_id
  ORDER BY max_price DESC
  LIMIT 10;
")

# Missing data
install.packages("naniar")
library(naniar)
gg_miss_var(customer)
gg_miss_var(orders)

top_expensive_products <- RSQLite::dbGetQuery(connection_new, "
SELECT i.product_id,
       COUNT(o.product_1) AS total_quantity_sold
FROM orders AS o
INNER JOIN inventory AS i ON o.product_1 = i.product_id
GROUP BY i.product_id
ORDER BY total_quantity_sold DESC
LIMIT 10;
")

# Top ad sites
top_ad_sites <- RSQLite::dbGetQuery(connection_new, "
  SELECT a.ad_site,
         COUNT(*) AS site_usage_count
  FROM advertisement AS a
  GROUP BY a.ad_site
  ORDER BY site_usage_count DESC
  LIMIT 10;
")

# Plot for top ad sites
plot_top_ad_sites <- ggplot(top_ad_sites, aes(x = forcats::fct_reorder(ad_site, site_usage_count), y = site_usage_count)) +
  geom_bar(stat = "identity") +
  theme(axis.text.x = element_text(angle = 90)) +
  xlab("Ad Site") +
  ylab("Site Usage Count") +
  coord_flip() + theme_minimal()

# Plot for top categories
plot_top_categories <- ggplot(top_cat, aes(x = forcats::fct_reorder(category_name, product_rating), y = product_rating)) +
  geom_bar(stat = "identity") +
  theme(axis.text.x = element_text(angle = 90)) +
  xlab("Category") +
  ylab("Total Rating") +
  coord_flip() + theme_minimal()

# Top sold products
top_sold_products <- RSQLite::dbGetQuery(connection_new, "
  SELECT p.product_id,
         p.product_name,
         COUNT(*) AS num_sales
  FROM inventory p
  INNER JOIN rating r ON p.product_id = r.product
  GROUP BY p.product_id, p.product_name
  ORDER BY num_sales DESC
  LIMIT 10;
")

# Plot for top sold products
plot_top_sold_products <- ggplot(top_sold_products, aes(x = forcats::fct_reorder(product_name, num_sales), y = num_sales)) +
  geom_bar(stat = "identity") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  xlab("Product") +
  ylab("Number of Sales") + theme_minimal()

# Top shipping methods
top_shipping_methods <- RSQLite::dbGetQuery(connection_new, "
  SELECT o.shipping_speed AS shipping_speed,
         COUNT(*) AS num_orders
  FROM shipping o
  GROUP BY o.shipping_speed
  ORDER BY num_orders DESC
  LIMIT 10;
")

# Plot for top shipping methods
plot_top_shipping_methods <- ggplot(top_shipping_methods, aes(x = forcats::fct_reorder(shipping_speed, num_orders), y = num_orders)) +
  geom_bar(stat = "identity") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  xlab("Shipping Speed") +
  ylab("Number of Orders") + theme_minimal()

# Top shipping couriers
top_shipping_couriers <- RSQLite::dbGetQuery(connection_new, "
  SELECT o.shipping_courier,
         COUNT(*) AS num_orders
  FROM shipping o
  WHERE o.shipping_courier IS NOT NULL  -- Filter out NULL courier service, if any
  GROUP BY o.shipping_courier
  ORDER BY num_orders DESC
  LIMIT 10;
")

# Plot for top shipping couriers
plot_top_shipping_couriers <- ggplot(top_shipping_couriers, aes(x = forcats::fct_reorder(shipping_courier, num_orders), y = num_orders)) +
  geom_bar(stat = "identity") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  xlab("Shipping Courier") +
  ylab("Number of Orders") + theme_minimal()

# Top shoe sizes
top_shoe_sizes <- RSQLite::dbGetQuery(connection_new, "
SELECT o.size_1,
       COUNT(*) AS num_orders
FROM orders o
WHERE o.size_1 IS NOT NULL  -- Filter out NULL shoe sizes, if any
GROUP BY o.size_1
ORDER BY num_orders DESC
LIMIT 10;
")

# Total sales by different ad platforms
sales_by_platform <- orders %>%
  inner_join(advertisement, by = c("ad_route" = "ad_id")) %>%
  group_by(ad_site) %>%
  summarize(TotalSales = sum(n()))

# Plot for total sales by different ad platforms
plot_sales_by_platform <- ggplot(sales_by_platform, aes(x = reorder(ad_site, TotalSales), y = TotalSales, fill = ad_site)) +
  geom_bar(stat = "identity") +
  labs(x = "Advertising Platform", y = "Total Sales", title = "Total Sales by different Advertisement Platforms") + theme_minimal()

# Advertisement performance
ads_performance <- advertisement %>%
  group_by(ad_site) %>%
  summarize(Total_Hits = sum(ad_hits)) %>%
  arrange(desc(Total_Hits))

# Plot for advertisement performance
plot_ads_performances <- ggplot(ads_performance, aes(x = reorder(ad_site, Total_Hits), y = Total_Hits, fill = ad_site)) +
  geom_bar(stat = "identity") +
  labs(title = "Advertisement Performance by Site", x = "Advertising Site", y = "Total Hits") + theme_minimal()

# Time series analysis for ad hits
ads <- advertisement %>% 
  mutate(ad_start_date = mdy(ad_start_date))

ad_hits_by_date <- ads %>%
  group_by(ad_start_date) %>%
  summarize(daily_hits = sum(ad_hits))

# Plot for time series analysis for ad hits
plot_ad_hits_by_date <- ggplot(ad_hits_by_date, aes(x = ad_start_date, y = daily_hits)) +
  geom_line() +
  labs(x = "Date", y = "Ad Hits", title = "Daily Ad Hits Over Time") + theme_minimal()

# Discount utilization
discount_utilization <- orders %>%
  group_by(discount_code) %>%
  summarize(Usage_Count = n())

# Plot for discount utilization
plot_discount_utilisation <- ggplot(discount_utilization, aes(x = discount_code, y = Usage_Count, fill = discount_code)) +
  geom_bar(stat = "identity") +
  labs(title = "Discount Code Usage", x = "Discount Code", y = "Usage Count") + theme_minimal()

# Most common transaction methods
transaction_counts <- transactions %>%
  group_by(payment_method) %>%
  summarize(Count = n()) %>%
  arrange(desc(Count)) %>%
  top_n(10)

# Plot for most common transaction methods
plot_payment_methods <- ggplot(transaction_counts, aes(x = reorder(payment_method, Count), y = Count, fill = payment_method)) +
  geom_bar(stat = "identity") +
  labs(title = "Most Common Transaction Methods", x = "Transaction Method", y = "Frequency") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1))

# Save plots
this_filename_date <- as.character(Sys.Date())
this_filename_time <- as.character(format(Sys.time(), format = "%H_%M"))

ggsave(paste0("analysis_graphs/plot_top_ad_sites",this_filename_date,"_",this_filename_time,".png"), plot = plot_top_ad_sites, width = 8, height = 6, units = "in")
ggsave(paste0("analysis_graphs/plot_top_categories",this_filename_date,"_",this_filename_time,".png"), plot = plot_top_categories, width = 8, height = 6, units = "in")
ggsave(paste0("analysis_graphs/plot_top_sold_products",this_filename_date,"_",this_filename_time,".png"), plot = plot_top_sold_products, width = 8, height = 6, units = "in")
ggsave(paste0("analysis_graphs/plot_top_shipping_methods",this_filename_date,"_",this_filename_time,".png"), plot = plot_top_shipping_methods, width = 8, height = 6, units = "in")
ggsave(paste0("analysis_graphs/plot_top_shipping_couriers",this_filename_date,"_",this_filename_time,".png"), plot = plot_top_shipping_couriers, width = 8, height = 6, units = "in")
ggsave(paste0("analysis_graphs/plot_sales_by_platform",this_filename_date,"_",this_filename_time,".png"), plot = plot_sales_by_platform, width = 8, height = 6, units = "in")
ggsave(paste0("analysis_graphs/plot_ads_performances",this_filename_date,"_",this_filename_time,".png"), plot = plot_ads_performances, width = 8, height = 6, units = "in")
ggsave(paste0("analysis_graphs/plot_ad_hits_by_date",this_filename_date,"_",this_filename_time,".png"), plot = plot_ad_hits_by_date, width = 8, height = 6, units = "in")
ggsave(paste0("analysis_graphs/plot_discount_utilisation",this_filename_date,"_",this_filename_time,".png"), plot = plot_discount_utilisation, width = 8, height = 6, units = "in")
ggsave(paste0("analysis_graphs/plot_payment_methods",this_filename_date,"_",this_filename_time,".png"), plot = plot_payment_methods, width = 8, height = 6, units = "in")

# Close the database connection
dbDisconnect(connection_new)


