 val glazedDonut = "Glazed Donut"
    val unitPrice = 2.50
    val qtyPurchased = 10
    val totalCost = qtyPurchased * unitPrice

    println(f"""Total cost of $qtyPurchased $glazedDonut${if (qtyPurchased > 1) "s" else ""} = $$$totalCost%1.2f""")
