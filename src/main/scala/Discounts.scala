import scala.io.Source
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.io.{File, PrintWriter}
import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import java.time.temporal.ChronoUnit
import java.sql.Timestamp
import java.io.{File, FileOutputStream, PrintWriter}

object Discounts extends App {

  // Logging function
  def log(level: String, message: String): Unit = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val logMessage = s"$timestamp $level $message"
    println(logMessage)
    // Writing to log file
    val writer = new PrintWriter(new FileOutputStream(new File("src/main/resources/rules_engine.log"), true))
    writer.println(logMessage)
    writer.close()
  }

  // Database interaction
  def writeToDb(orders: List[String], rules: List[(Order => Boolean, Order => Double)], writer: PrintWriter, f: File): Unit = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    val url = "jdbc:oracle:thin:@//localhost:1521/XE"
    val username = "ROOT"
    val password = "root"

    val insertStatement =
      """
        |INSERT INTO orders (order_date, expiry_date,
        |                   product_name, quantity, unit_price, channel, payment_method,
        |                   discount, total_price)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver") // Load the Oracle JDBC driver
      connection = DriverManager.getConnection(url, username, password)
      log("DEBUG", "Successfully opened database connection")
      // Prepare the INSERT statement
      preparedStatement = connection.prepareStatement(insertStatement)

      // Insert data into the table
      orders.foreach {  order =>
        val orderData = order.split(",")
        val orderDate = Timestamp.valueOf(orderData(0)) // Convert orderDate string to Timestamp
        val expiryDate = Date.valueOf(orderData(1)) // Convert expiryDate string to Date
        val productName = orderData(2)
        val quantity = orderData(3).toInt
        val unitPrice = orderData(4).toDouble
        val channel = orderData(5)
        val paymentMethod = orderData(6)
        val discount = orderData(7).toDouble
        val finalPrice = orderData(8).toDouble
        preparedStatement.setTimestamp(1, orderDate) // Set orderDate as Timestamp
        preparedStatement.setDate(2, expiryDate) // Set expiryDate as Date
        preparedStatement.setString(3, productName)
        preparedStatement.setInt(4, quantity)
        preparedStatement.setDouble(5, unitPrice)
        preparedStatement.setString(6, channel)
        preparedStatement.setString(7, paymentMethod)
        preparedStatement.setDouble(8, discount)
        preparedStatement.setDouble(9, finalPrice)
        preparedStatement.addBatch()
      }

      // Execute the batch of INSERT statements
      preparedStatement.executeBatch()
    } catch {
      case e: Exception =>
        log("ERROR", s"Failed to insert into database: ${e.getMessage}")
    } finally {
      // Close resources
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
      log("INFO", "Successfully inserted into database")
      log("DEBUG", "Closed database connection")
    }
  }

  // Define case classes for representing orders and discounts
  case class Order(orderDate: Timestamp, expiryDate: LocalDate, productCategory: String, productName: String,
                   quantity: Int, unitPrice: Double, channel: String, paymentMethod: String)

  // Define functions to check qualifying rules
  def lessThan30DaysRemaining(order: Order): Boolean = {
    val daysToExpiry = ChronoUnit.DAYS.between(order.orderDate.toLocalDateTime.toLocalDate, order.expiryDate)
    daysToExpiry < 30
  }

  def isCheeseOrWine(order: Order): Boolean = {
    order.productCategory.startsWith("Cheese") || order.productCategory.startsWith("Wine")
  }

  def soldOn23rdMarch(order: Order): Boolean = {
    order.orderDate.toLocalDateTime.toLocalDate.getMonthValue == 3 && order.orderDate.toLocalDateTime.toLocalDate.getDayOfMonth == 23
  }

  def moreThan5Units(order: Order): Boolean = {
    order.quantity > 5
  }

  // Define a function to check if an order was made through the App channel
  def quaChannel(order: Order): Boolean = order.channel == "App"

  // Define a function to calculate the discount based on the channel
  def calChannel(order: Order): Double = {
    val quantity = order.quantity
    val roundedQuantity = math.ceil(quantity / 5.0) * 5
    val discountPercent = (roundedQuantity / 5) * 0.05
    discountPercent
  }

  // Define a function to check if an order's payment method is Visa
  def quaPayMethod(order: Order): Boolean = order.paymentMethod== "Visa"

  // Define a function to calculate the discount for Visa payments
  def calPayMethod(order: Order): Double = 0.05
  // Define functions to calculate discounts
  def calculateExpiryDiscount(order: Order): Double = {
    val daysToExpiry = ChronoUnit.DAYS.between(order.orderDate.toLocalDateTime.toLocalDate, order.expiryDate)
    if (daysToExpiry < 30) {
      val discount = (30 - daysToExpiry) * 0.01
      discount
    }
    else 0.0
  }

  def calculateCheeseWineDiscount(order: Order): Double = {
    if (order.productName.startsWith("Cheese")) 10 else if (order.productName.startsWith("Wine")) 5 else 0
  }

  def calculate23rdMarchDiscount(order: Order): Double = {
    0.5
  }

  def calculateQuantityDiscount(order: Order): Double = {

    if (order.quantity >= 6 && order.quantity <= 9) 0.05
    else if (order.quantity >= 10 && order.quantity <= 14) 0.07
    else if (order.quantity >= 15) 0.1
    else 0.0
  }

  // Create a list of tuples where each tuple contains two functions: one for the qualifying rule and one for the discount calculation
  val qualifyDiscountMap: List[(Order => Boolean, Order => Double)] = List(
    (lessThan30DaysRemaining, calculateExpiryDiscount),
    (isCheeseOrWine, calculateCheeseWineDiscount),
    (soldOn23rdMarch, calculate23rdMarchDiscount),
    (moreThan5Units, calculateQuantityDiscount),
    (quaChannel, calChannel), // New rule for App purchases
    (quaPayMethod, calPayMethod) // New rule for Visa card payments

  )

  // Define a function to apply rules to a list of orders and return processed orders
  def applyRulesToOrders(orders: List[Order],rulesList: List[(Order => Boolean, Order => Double)]): List[String] = {
    log("INFO",s"   Log Level: Event   Message:Starting applying rules")
    // Process each order
    val processedLines: List[String] = orders.map { line =>
      // Apply each rule and collect the results
      val appliedRules = rulesList.collect {
        case (condition, calculation) if condition(line) => calculation(line)
      }
      // Select the top two discounts
      val topTwo = appliedRules.sorted.takeRight(2)
      // Calculate the overall discount
      val discount = if (topTwo.nonEmpty) {
        if (topTwo.length == 1) {
          topTwo.head / 1.toDouble
        } else {
          topTwo.sum / 2.toDouble
        }
      } else {
        0.0
      }
      // Log whether the order has a discount
      if (discount != 0.0) {
        log("Info",s"   Log Level: Info   Message:This order has a discount= $discount")
      } else {
        log("Info",s"   Log Level: Info   Message:this order has no discounts")
      }
      // Calculate the final price after applying the discount
      val qty = line.quantity
      val finalPrice = (qty * line.unitPrice) - ((qty * line.unitPrice) * discount )
      // Return the processed order line
      s"${line.orderDate},${line.expiryDate},${line.productName},${line.quantity},${line.unitPrice},${line.channel},${line.paymentMethod},$discount,$finalPrice\n"
    }
    processedLines
  }

  // Parse each line into an Order object
  val lines = Source.fromFile("src/main/resources/TRX1000.csv").getLines().toList.tail
  val orders = lines.map(parseOrderLine)

  // Apply discounts and calculate final prices
  val discountedOrders = applyRulesToOrders(orders, qualifyDiscountMap)

  // Calculate final prices and load the result into a database table
  val writer = new PrintWriter(new FileOutputStream(new File("log.txt"), true))
  writeToDb(discountedOrders, qualifyDiscountMap, writer, new File("file.txt"))

  // Logging successful insertion
  log("INFO", "All orders processed and inserted into the database")

  // Close the writer
  writer.close()

  // Parse a line from CSV into an Order object
  def parseOrderLine(line: String): Order = {
    val Array(orderDate, productName, expiryDate, quantity, unitPrice, channel, paymentMethod) = line.split(",")
    val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val localDateTime = LocalDateTime.parse(orderDate, timestampFormatter)
    val timestamp = Timestamp.valueOf(localDateTime)
    Order(timestamp, LocalDate.parse(expiryDate), productName, productName, quantity.toInt, unitPrice.toDouble, channel, paymentMethod)
  }



}
