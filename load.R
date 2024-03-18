library(readr)
library(dplyr)
library(RSQLite)
library(readxl)
library(openxlsx)

connection_new <- RSQLite::dbConnect(RSQLite::SQLite(), "new_database.db") 

# Physical Schema
tables <- RSQLite::dbListTables(connection_new)



RSQLite::dbExecute(connection_new,"

CREATE TABLE IF NOT EXISTS 'category' ( 
  
  'category_id' INT NOT NULL PRIMARY KEY, 
  
  'category_name' TEXT NOT NULL 
  
); ")



RSQLite::dbExecute(connection_new,"

CREATE TABLE IF NOT EXISTS inventory ( 
  
  product_id INT PRIMARY KEY, 
  
  product_name TEXT NOT NULL, 
  
  colour TEXT NOT NULL, 
  
  price DECIMAL(5,2) NOT NULL, 
  
  category INT NOT NULL, 
  
  category_2 INT, 
  
  FOREIGN KEY (category)REFERENCES category ('category_id'), 
  
  FOREIGN KEY (category_2) REFERENCES category('category_id') 
  
); ")


RSQLite::dbExecute(connection_new,"

CREATE TABLE IF NOT EXISTS advertisement ( 
  
  ad_id INT PRIMARY KEY,  
  
  ad_site TEXT NOT NULL, 
  
  ad_start_date DATE NOT NULL, 
  
  ad_end DATE NOT NULL, 
  
  ad_hits INT, 
  
  product_id INT NOT NULL, 
  
  FOREIGN KEY (product_id) REFERENCES inventory(product_id) 
  
); ")





RSQLite::dbExecute(connection_new," 

CREATE TABLE IF NOT EXISTS customer ( 
  
  email TEXT NOT NULL, 
  
  first_name VARCHAR(50), 
  
  last_name VARCHAR(50), 
  
  customer_id INT PRIMARY KEY, 
  
  member TINYINT NOT NULL, 
  
  address VARCHAR(50) NOT NULL, 
  
  postcode VARCHAR(50) NOT NULL, 
  
  reg_date DATETIME DEFAULT CURRENT_TIMESTAMP, 
  
  phone_num VARCHAR(20)  
  
); 
")



RSQLite::dbExecute(connection_new,"

CREATE TABLE IF NOT EXISTS shipping ( 
  
  shipping_id INT PRIMARY KEY , 
  
  shipping_courier VARCHAR(100) NOT NULL, 
  
  shipping_speed TEXT NOT NULL,  
  
  shipping_cost DECIMAL(5,2) NOT NULL,
  
  order_id NOT NULL,
  
  FOREIGN KEY (order_id) REFERENCES orders(order_id) 
  
); ")



RSQLite::dbExecute(connection_new,"

CREATE TABLE IF NOT EXISTS rating ( 
  
  review_id  VARCHAR(50) PRIMARY KEY, 
  
  rating INT, 
  
  product INT, 
  
  CHECK (rating >= 1 AND rating <= 5), 
  
  FOREIGN KEY (product) REFERENCES Inventory(product_id) 
  
); 
")


RSQLite::dbExecute(connection_new,"

CREATE TABLE IF NOT EXISTS discount ( 
  
  discount_code VARCHAR(50) PRIMARY KEY, 
  
  amount DECIMAL(2,2) 
  
); ")



RSQLite::dbExecute(connection_new," 

CREATE TABLE IF NOT EXISTS orders( 
  
  order_id VARCHAR(15) PRIMARY KEY, 
  
  product_1 INT NOT NULL, 
  
  product_2 INT, 
  
  product_3 INT, 
  
  product_4 INT, 
  
  product_5 INT, 
  
  size_1 INT NOT NULL, 
  
  size_2 INT, 
  
  size_3 INT, 
  
  size_4 INT, 
  
  size_5 INT, 
  
  discount_code VARCHAR(50), 
  
  customer_id NOT NULL, 
  
  ad_route INT,  
  
  FOREIGN KEY (discount_code) REFERENCES discount(discount_code), 
  
  FOREIGN KEY (customer_id) REFERENCES customer(customer_id) 
  
); ")





RSQLite::dbExecute(connection_new,"

CREATE TABLE IF NOT EXISTS transactions( 
  
  transaction_id VARCHAR(20) PRIMARY KEY, 
  
  payment_method VARCHAR(50), 
  
  card_no INT NOT NULL, 
  
  order_id INT, 
  
  FOREIGN KEY(order_id) REFERENCES Order_('order_id') 
  
); ")

advertisement<- read.csv("shoes_data/advertisement/advertisement.csv") 

category<-  read.csv("shoes_data/category/category.csv") 

customer<-  read.csv("shoes_data/customer/customer.csv") 

discount<-  read.csv("shoes_data/discount/discount.csv") 

inventory<-  read.csv("shoes_data/inventory/inventory.csv") 

orders<-  read.csv("shoes_data/orders/orders.csv") 

rating<-  read.csv("shoes_data/rating/rating.csv") 

shipping<-  read.csv("shoes_data/shipping/shipping.csv") 

transactions <-  read.csv("shoes_data/transactions/transactions.csv") 


# Define the path to the data_upload folder
data_upload_path <- "shoes_data"

# Get a list of all subdirectories within data_upload
subdirectories <- list.dirs(data_upload_path, full.names = TRUE, recursive = FALSE)

# Iterate through each subdirectory
for (entity_folder in subdirectories) {
  # Extract the entity name from the directory path
  entity_name <- basename(entity_folder)
  # Get a list of all CSV files within the entity folder
  csv_files <- list.files(entity_folder, pattern = "*.csv", full.names = TRUE)
  # Initialize an empty dataframe to store the merged data
  merged_df <- NULL
  # Iterate through each CSV file
  for (csv_file in csv_files) {
    # Read the CSV file into a dataframe
    df <- read_csv(csv_file)
    # Merge the dataframe with the existing merged dataframe
    if (is.null(merged_df)) {
      merged_df <- df
    } else {
      merged_df <- bind_rows(merged_df, df)
    }
  }
  # Assign the merged dataframe to a variable with the entity name
  assign(paste0("ecom_", entity_name), merged_df, envir = .GlobalEnv)
}

# Print the names of the created dataframes
print(ls(pattern = "ecom_"))

#Data checks

library(DBI)
# Check data integrity and validity

connection_new <- RSQLite::dbConnect(RSQLite::SQLite(), "new_database.db")

# Check data types in the discount table
discount_check <- dbGetQuery(connection_new, "
                        SELECT typeof(discount_code), typeof(amount) 
                        FROM discount
                        WHERE typeof(amount) != 'real'
                        OR typeof(discount_code) != 'text'
                        ")

# Check data types in the rating table
rating_check <- dbGetQuery(connection_new, "
                        SELECT typeof(review_id), typeof(rating), typeof(product) 
                        FROM rating
                        WHERE typeof(rating) != 'integer' 
                        OR typeof(review_id) != 'text'
                        OR typeof(product) != 'integer'
                        ")

# Check data types in the customer table
customer_check <- dbGetQuery(connection_new, "
                        SELECT typeof(email), typeof(first_name), typeof(last_name), typeof(customer_id), typeof(member),
                        typeof(address), typeof(postcode), typeof(reg_date), typeof(phone_num)
                        FROM customer
                        WHERE typeof(email) != 'text'
                        OR typeof(first_name) NOT IN ('text', 'null')
                        OR typeof(last_name) NOT IN ('text', 'null')
                        OR typeof(customer_id) != 'text'
                        OR typeof(member) != 'text'
                        OR typeof(address) != 'text'
                        OR typeof(postcode) != 'text'
                        OR typeof(reg_date) != 'text'
                        OR typeof(phone_num) NOT IN ('text','null')
                        ")

# Check data types in the category table
category_check <- dbGetQuery(connection_new, "
                        SELECT typeof(category_id), typeof(category_name)
                        FROM category
                        WHERE typeof(category_id) != 'integer'
                        OR typeof(category_name) != 'text'
                        ")

# Check data types in the inventory table
inventory_check <- dbGetQuery(connection_new, "
                        SELECT typeof(product_id), typeof(product_name), typeof(colour), typeof(price),
                        typeof(category), typeof(category_2)
                        FROM inventory
                        WHERE typeof(product_id) != 'integer'
                        OR typeof(product_name) != 'text'
                        OR typeof(colour) != 'text'
                        OR typeof(price) NOT IN ('real', 'integer')
                        OR typeof(category) != 'integer'
                        OR typeof(category_2) NOT IN ('integer','null') 
                        ")

# Check data types in the orders table
orders_check <- dbGetQuery(connection_new, "
                        SELECT typeof(order_id), typeof(product_1), typeof(product_2), typeof(product_3),
                        typeof(product_4), typeof(product_5), typeof(size_1), typeof(size_2), typeof(size_3), typeof(size_4), typeof(size_5), typeof(discount_code), typeof(customer_id), typeof(ad_route)
                        FROM orders
                        WHERE typeof(order_id) != 'text'
                        OR typeof(product_1) != 'integer'
                        OR typeof(product_2) NOT IN ('integer', 'null')
                        OR typeof(product_3) NOT IN ('integer', 'null')
                        OR typeof(product_4) NOT IN ('integer', 'null')
                        OR typeof(product_5) NOT IN ('integer', 'null')
                        OR typeof(size_1) NOT IN ('integer', 'real')
                        OR typeof(size_2) NOT IN ('integer', 'real', 'null')
                        OR typeof(size_3) NOT IN ('integer', 'real', 'null')
                        OR typeof(size_4) NOT IN ('integer', 'real', 'null')
                        OR typeof(size_5) NOT IN ('integer', 'real', 'null')
                        OR typeof(discount_code) NOT IN ('null', 'text')
                        OR typeof(customer_id) != 'text'
                        OR typeof(ad_route) NOT IN ('integer','null')
                        ")

# Check data types in the shipping table
shipping_check <- dbGetQuery(connection_new, "
                        SELECT typeof(shipping_id), typeof(shipping_cost), typeof(shipping_speed), typeof(shipping_courier), typeof(order_id)
                        FROM shipping
                        WHERE typeof(shipping_id) != 'integer'
                        OR typeof(shipping_cost) != 'real'
                        OR typeof(shipping_speed) != 'text'
                        OR typeof(shipping_courier) != 'text'
                        OR typeof(order_id) != 'text'
                        ")

# Check data types in the transactions table
transactions_check <- dbGetQuery(connection_new, "
                        SELECT typeof(transaction_id), typeof(payment_method), typeof(card_no), typeof(order_id)
                        FROM transactions
                        WHERE typeof(transaction_id) != 'text'
                        OR typeof(payment_method) != 'text'
                        OR typeof(card_no) != 'integer'
                        OR typeof(order_id) != 'text'
                        ")

#Check uniqueness in the product_name column of the inventory table
unique_product_names <- dbGetQuery(connection_new, "
                                 SELECT COUNT(DISTINCT product_name) AS total_products
                                 FROM inventory
                                 ")

# Check uniqueness in the email column of the customer table
unique_emails <- dbGetQuery(connection_new, "
                                 SELECT COUNT(DISTINCT email) AS total_emails
                                 FROM customer
                                 ")

# Check assumptions about minimum and maximum price in the inventory table
price_range <- dbGetQuery(connection_new, "
                                 SELECT MIN(price) AS minimum_price, MAX(price) AS maximum_price
                                 FROM inventory
                                 ")

# Check ads with duration between 10 and 60 days, starting and ending within specified date range
valid_ads <- dbGetQuery(connection_new, "
                                 SELECT *
                                   FROM advertisement
                                 WHERE 
                                 (julianday(ad_end) - julianday(ad_start_date)) BETWEEN 10 AND 60
                                 AND ad_start_date BETWEEN '2023-09-01' AND '2024-01-31'
                                 AND ad_end BETWEEN '2023-09-01' AND '2024-01-31'
                                 ")

# Check orders with sizes within a specified range
valid_orders <- dbGetQuery(connection_new, "
                                 SELECT *
                                   FROM orders
                                 WHERE 
                                 NOT (
                                   (size_1 BETWEEN 4 AND 12)
                                   AND (size_2 BETWEEN 4 AND 12)
                                   AND (size_3 BETWEEN 4 AND 12)
                                   AND (size_4 BETWEEN 4 AND 12)
                                   AND (size_5 BETWEEN 4 AND 12)
                                 )
                                 ")

# Check if all customers who ordered a product are on the customer list
customer_order_check <- dbGetQuery(connection_new, "
                                 SELECT orders.customer_id, customer.customer_id
                                 FROM orders, customer
                                 WHERE orders.customer_id = customer.customer_id
                                 ")

# Check if all products ordered exist in the inventory list
product_order_check1 <- dbGetQuery(connection_new, "
                                 SELECT orders.product_1, inventory.product_id
                                 FROM orders, inventory
                                 WHERE orders.product_1 = inventory.product_id
                                 ")

# Check if all products ordered exist in the inventory list (for product_2)
product_order_check2 <- dbGetQuery(connection_new, "
                                 SELECT orders.product_2, inventory.product_id
                                 FROM orders, inventory
                                 WHERE orders.product_2 = inventory.product_id
                                 ")

# Check if all products ordered exist in the inventory list (for product_3)
product_order_check3 <- dbGetQuery(connection_new, "
                                 SELECT orders.product_3, inventory.product_id
                                 FROM orders, inventory
                                 WHERE orders.product_3 = inventory.product_id
                                 ")

# Check if all products ordered exist in the inventory list (for product_4)
product_order_check4 <- dbGetQuery(connection_new, "
                                 SELECT orders.product_4, inventory.product_id
                                 FROM orders, inventory
                                 WHERE orders.product_4 = inventory.product_id
                                 ")

# Check if all products ordered exist in the inventory list (for product_5)
product_order_check5 <- dbGetQuery(connection_new, "
                                 SELECT orders.product_5, inventory.product_id
                                 FROM orders, inventory
                                 WHERE orders.product_5 = inventory.product_id
                                 ")

# Check if each ad has a correct product_id from the inventory table
ad_product_check <- dbGetQuery(connection_new, "
                                 SELECT inventory.product_id, advertisement.product_id
                                 FROM inventory, advertisement
                                 WHERE inventory.product_id = advertisement.product_id
                                 ")

# Check if each rating refers to an existing product id from the inventory table
rating_product_check <- dbGetQuery(connection_new, "
                                 SELECT inventory.product_id, rating.product
                                 FROM inventory, rating
                                 WHERE inventory.product_id = rating.product
                                 ")

# Calculate the number of items ordered
number_of_items_ordered <- dbGetQuery(connection_new, "
                                 SELECT o.order_id,
                                 SUM(CASE WHEN o.product_1 >= 0 THEN 1
                                     WHEN o.product_1 IS NULL THEN 0 
                                     ELSE 0 END)+
                                   SUM(CASE WHEN o.product_2 >= 0 THEN 1
                                       WHEN o.product_2 IS NULL THEN 0 
                                       ELSE 0 END)+ 
                                   SUM(CASE WHEN o.product_3 >= 0 THEN 1
                                       WHEN o.product_3 IS NULL THEN 0 
                                       ELSE 0 END) +
                                   SUM(CASE WHEN o.product_4 >= 0 THEN 1
                                       WHEN o.product_4 IS NULL THEN 0 
                                       ELSE 0 END) +
                                   SUM(CASE WHEN o.product_5 >= 0 THEN 1
                                       WHEN o.product_5 IS NULL THEN 0 
                                       ELSE 0 END) 
                                 AS number_of_items
                                 FROM orders o
                                 GROUP BY o.order_id
                                 ")

# Calculate basket price
basket_price <- dbGetQuery(connection_new, "
                                 WITH OrderedItems AS (
                                SELECT o.order_id,
                                       o.product_1 AS item_id,
                                       COALESCE(i1.price, 0) AS price
                                FROM orders o
                                LEFT JOIN inventory i1 ON o.product_1 = i1.product_id
                              
                                UNION ALL
                              
                                SELECT o.order_id,
                                       o.product_2 AS item_id,
                                       COALESCE(i2.price, 0) AS price
                                FROM orders o
                                LEFT JOIN inventory i2 ON o.product_2 = i2.product_id
                              
                                UNION ALL
                              
                                SELECT o.order_id,
                                       o.product_3 AS item_id,
                                       COALESCE(i3.price, 0) AS price
                                FROM orders o
                                LEFT JOIN inventory i3 ON o.product_3 = i3.product_id
                              
                                UNION ALL
                              
                              SELECT o.order_id,
                                       o.product_4 AS item_id,
                                       COALESCE(i4.price, 0) AS price
                                FROM orders o
                                LEFT JOIN inventory i4 ON o.product_4 = i4.product_id
                              
                                UNION ALL
                              
                                SELECT o.order_id,
                                       o.product_5 AS item_id,
                                       COALESCE(i5.price, 0) AS price
                                FROM orders o
                                LEFT JOIN inventory i5 ON o.product_5 = i5.product_id
                              )
                              
                              SELECT o.order_id,
                                SUM(COALESCE(oi.price, 0)) AS basket_price,
                                ROUND((
                                  SUM(COALESCE(oi.price, 0)) * (1 - d.amount)
                                ) + s.shipping_cost, 2) AS transaction_amount
                              FROM orders o
                              
                              INNER JOIN OrderedItems oi ON o.order_id = oi.order_id
                              INNER JOIN discount d ON o.discount_code = d.discount_code 
                              INNER JOIN shipping s ON o.order_id = s.order_id
                              GROUP BY o.order_id;
                                 ")


# Calculate average product ratings
average_ratings <- dbGetQuery(connection_new, "
                                 SELECT p.product_id,
                                 ROUND(AVG(r.rating), 1) AS product_rating
                                 FROM inventory p
                                 INNER JOIN rating r ON p.product_id = r.product
                                 GROUP BY p.product_id
                                 ")


#Data validation
#{r Data Validation for Quality and Integrity : Customer}

missing_values <- apply(is.na(customer), 2, sum)

# Check unique customer IDs
if (length(unique(customer$customer_id)) != nrow(customer)) {
  print("Customer ID is not unique.")
}

# Check data types for first_name and last_name
if (!all(sapply(customer$first_name, is.character)) || !all(sapply(customer$last_name, is.character))) {
  print("First name and last name should be character.")
}

# Check email format
if (any(!grepl("^\\S+@\\S+\\.\\S+$", customer$email))) {
  print("Invalid email format")
}

# If no errors are found, print a message indicating that the data is valid
# Check if there are any errors in the data
if (!any(is.na(missing_values)) && 
    length(unique(customer$customer_id)) == nrow(customer) &&
    all(sapply(customer$first_name, is.character)) &&
    all(sapply(customer$last_name, is.character)) &&
    all(grepl("^\\S+@\\S+\\.\\S+$", customer$email))) {
  
  # If no errors are found, print a message indicating that the data is valid
  print("Data is valid.")
  
  # Load the data into the database
} else {
  # If errors are found, print a message indicating that the data is not valid
  print("Data is not valid. Please correct the errors.")
}


#{r Data Validation for Quality and Integrity : advertisement }
na_disc <- apply(is.na(discount), 2, sum)

# Check discount percentage range (assuming it's between 0 and 100)
if (any(discount$amount < 0 | discount$amount > 1)) {
  print("Invalid discount percentage.")
}

# If no errors are found, print a message indicating that the data is valid
if (!any(is.na(na_disc)) &&
    all(discount$amount > 0 | discount$amount < 1) &&
    !any(!sapply(discount$discount_code, is.character))) {
  print("Discount data is valid.")
  # Load the data into the database
} else {
  print("Discount data is not valid. Please correct the errors.")
}

#{r Data Validation for Quality and Integrity : Order}

na_order <- apply(is.na(orders), 2, sum)

if (any(!orders$product_id %in% inventory$product_id)) {
  print("Invalid product IDs. Some product IDs do not exist in the Product table.")
}
if (any(!orders$customer_id %in% customer$customer_id)) {
  print("Invalid customer IDs. Some customer IDs do not exist in the Customer table.")
}
# check ordered products exist in the inventory table
violations <- FALSE

for (i in 1:nrow(orders)) {
  for (j in 1:5) {
    product_id <- orders[i, paste0("product_", j)]
    if (is.na(product_id) || product_id == "") {
      next
    }
    if (!product_id %in% inventory$product_id) {
      violations <- TRUE
      cat("Row", i, ": Product", j, "(", product_id, ") does not exist in the inventory.\n")
    }
  }
}
if (!violations) {
  print("All products exist in the inventory.")
}

# If no errors are found, print a message indicating that the data is valid
if (!any(is.na(na_order)) && 
    all(orders$customer_id %in% customer$customer_id)) {
  print("Order data is valid.")
  # Load the data into the database
} else {
  print("Order data is not valid. Please correct the errors.")
}


#{r Data Validation for Quality and Integrity : Product_Category}
na_prod_cat <- apply(is.na(category), 2, sum)

# Ensure "category_id" values are unique
if (length(unique(category$category_id)) != nrow(category)) {
  print("category_id values are not unique.")
}


# Check data type of each column
if (!all(sapply(category$category_id, is.numeric)) ||
    !all(sapply(category$category_name, is.character)) ) {
  print("Invalid data type for one or more columns.")
}

# If no errors are found, print a message indicating that the data is valid
if (!any(is.na(na_prod_cat)) &&
    length(unique(category$category_id)) == nrow(category) &&
    !any(nchar(category$category_name) > 255) &&
    all(sapply(category$category_id, is.numeric)) &&
    all(sapply(category$category_name, is.character)) ) {
  print("product_category data is valid.")
  # Load the data into the database
} else {
  print("product_category data is not valid. Please correct the errors.")
}

#{r Data Validation for Quality and Integrity : Products}
na_Product <- apply(is.na(inventory), 2, sum)

# Ensure "product_id" values are unique
if (length(unique(inventory$product_id)) != nrow(inventory)) {
  print("product_id values are not unique.")
}

# Check length of "product_name"
if (any(nchar(inventory$product_name) > 255)) {
  print("product_name exceeds 255 characters.")
}

if (any(!inventory$category %in% category$category_id)) {
  print("Invalid category IDs. Some category IDs do not exist in the product_category table.")
}


# If no errors are found, print a message indicating that the data is valid
if (!any(is.na(na_Product)) &&
    length(unique(inventory$product_id)) == nrow(inventory) &&
    !any(nchar(inventory$product_name) > 255) &&
    all(inventory$category %in% category$category_id)) {
  print("Product data is valid.")
  # Load the data into the database
} else {
  print("Product data is not valid. Please correct the errors.")
}


#{r Data Validation for Quality and Integrity : Shipment}
na_shipment <- sapply(shipping, function(x) sum(is.na(x)))

# Ensure "shipment_id" values are unique
if (length(unique(shipping$shipping_id)) != nrow(shipping)) {
  print("shipment_id values are not unique.")
}

# Validate "shipment_speed" = columns
if (any(shipping$shipping_speed <= 0)) {
  print("shipment period should be positive numbers.")
}

# Validate "shipment_cost"
if (any(shipping$shipping_cost <= 0)) {
  print("shipment cost should be positive numbers.")
}

# Ensure that all "order_number" values exist in the "Order" table
order_numbers <- unique(shipping$order_id)
if (!all(order_numbers %in% orders$order_id)) {
  print("Some order numbers do not exist in the 'Order' table.")
}

# Validate "shipment_courier"
if (any(shipping$shipping_cost <= 0)) {
  print("shipment cost should be positive numbers.")
}

if (!all(sapply(shipping$shipping_courier, is.character)) ||
    !all(sapply(category$shipping_id, is.numeric)) ) {
  print("Invalid data type for one or more columns.")
}

# If no errors are found, print a message indicating that the data is valid
if (all(na_shipment == 0) &&
    all(shipping$shipping_speed > 0) &&
    all(shipping$shipment_cost > 0) &&
    all(order_numbers %in% orders$order_id)) {
  print("Shipment data is valid.")
  # Load the data into the database
} else {
  print("Shipment data is not valid. Please correct the errors.")
}

RSQLite::dbWriteTable(connection_new,"advertisement",advertisement,append=TRUE)
RSQLite::dbWriteTable(connection_new,"category",category,append=TRUE)
RSQLite::dbWriteTable(connection_new,"customer",customer,append=TRUE)
RSQLite::dbWriteTable(connection_new,"discount",discount,append=TRUE)
RSQLite::dbWriteTable(connection_new,"inventory",inventory,append=TRUE)
RSQLite::dbWriteTable(connection_new,"orders",orders,append=TRUE)
RSQLite::dbWriteTable(connection_new,"rating",rating,append=TRUE)
RSQLite::dbWriteTable(connection_new,"shipping",shipping,append=TRUE)
RSQLite::dbWriteTable(connection_new,"transactions",transactions,append=TRUE)

# List all entity folders in the shoes_data directory
entity_folders <- list.files("shoes_data", full.names = TRUE)

# Create a list to store data frames for each entity
entity_data <- list()

# Loop over each entity folder
for (folder in entity_folders) {
  # Extract the entity name from the folder path
  entity_name <- basename(folder)
  
  # List all CSV files within the entity folder
  csv_files <- list.files(folder, pattern = "\\.csv$", full.names = TRUE)
  
  # Read each CSV file and store it in a data frame
  entity_df <- lapply(csv_files, read.csv)
  
  # Combine all data frames into a single data frame
  entity_df <- do.call(rbind, entity_df)
  
  # Assign the data frame to the list with the entity name as the key
  entity_data[[paste0("shoe_", entity_name)]] <- entity_df
}

# Now, entity_data contains data frames for each entity with names like "shoe_entity_name"
# You can access each data frame using entity_data$shoe_entity_name

