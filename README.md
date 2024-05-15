# Scala-Discounts-Real-Time-Rule-Engine

## Overview

This Scala project is designed to handle discounts for orders based on various qualifying rules. It reads order data from a CSV file, applies discounts according to predefined rules, calculates final prices, and inserts the processed data into a database. Additionally, it includes logging functionality to track the processing flow and outcomes.

## Features

- **Order Processing**: Parses order data from a CSV file into `Order` objects.
- **Discount Calculation**: Applies discounts based on several qualifying rules such as expiry date, product category, quantity, channel, and payment method.
- **Logging**: Provides logging functionality to record processing events and outcomes, aiding in debugging and monitoring.
- **Database Interaction**: Inserts the processed order data into a database for further analysis and reporting.

## Qualifying Rules
The project implements the following qualifying rules for discounts:

- **Expiry Date**: Discounts are applied if the expiry date is less than 30 days from the order date.
- **Product Category**: Discounts are applied to products categorized as "Cheese" or "Wine".
- **Order Date**: Discounts are applied if the order date falls on the 23rd of March.
- **Quantity**: Discounts are applied based on the quantity of products ordered.
- **Channel**: Additional discounts are offered for orders made through the "App" channel.
- **Payment Method**: Discounts are offered for orders paid with a "Visa" card.
## Getting Started

### Prerequisites

- Scala (version X.X.X)
- Java Development Kit (JDK)
- Oracle Database (optional)

### Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/SalmaAhmed6/Scala-Discounts-Real-Time-Rule-Engine
    ```

2. Navigate to the project directory:

    ```bash
    cd discounts-scala-project
    ```

3. Compile the Scala code:

    ```bash
    scalac Discounts.scala
    ```

4. Run the application:

    ```bash
    scala Discounts
    ```

