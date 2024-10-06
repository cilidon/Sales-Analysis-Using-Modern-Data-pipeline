# Sales Analysis Using Modern Data pipeline

## 1. Introduction
This project presents a comprehensive analysis of sales revenue and customer behavior for a retail business, utilizing data from the Tableau Superstore dataset. The analysis aims to provide actionable insights to drive business growth and optimize performance.

## 2. Background
The primary objective of this project was to address the challenge of understanding and improving sales performance and customer engagement. By analyzing key metrics and trends, the business sought to:
- Identify areas for growth
- Optimize product offerings
- Enhance customer relationships

The potential business impact includes increased revenue, improved customer retention, and more effective resource allocation.

## 3. Dataset Overview
The analysis is based on the Tableau Superstore dataset, which contains:
- Transactional data for 2021, 2022, and 2023
- Information on customers, orders, sales, profits
- Product categories and subcategories

This comprehensive dataset allows for a multifaceted analysis of both customer behavior and sales performance.

The data was extracted and transformed using DBT(Data Build Tool) that incuded macros and tests for data accuracy and integrity. The data was then stored in snowflake for warehousing. 
This was all orchestrated using prefect.


## 4. Executive Summary

Sales Dashborad:

![Screenshot 2024-10-06 165047](https://github.com/user-attachments/assets/e937e9e1-3628-4041-b5f6-eaca9392f2c7)

Customer Dashboard: 
![Screenshot 2024-10-06 171151](https://github.com/user-attachments/assets/77e7f34d-7ea9-466c-b6f5-a057bfa1689a)


Dashboard can be found [here](https://public.tableau.com/app/profile/mandar.dilip.mhatre/viz/Sales_analysis_17216992735700/SalesDashboard)

Key findings from the analysis include:
- Total sales increased by 29.5% to $609,000 in 2023
- Profit grew by 32.7% to $82,000
- Customer base expanded by 11.3% to 693
- Total orders increased by 26.7%
- Average sales per customer rose by 16.3% to $955
- Strong performance in product categories like Chairs, Phones, and Tables
- Opportunities identified for optimizing certain product categories
- Customer ordering patterns indicate potential for improving repeat business

## 5. Insights Deep Dive
- Customer base growth (11.3%) was outpaced by order growth (26.7%), indicating increased customer engagement
- Sales per customer increased by 16.3%, suggesting successful upselling or higher-value purchases
- 37.7% of customers (261) made only one order, presenting an opportunity for improving customer retention
- Top 10 customers by profit show high variability in order frequency (2-3 orders) and profit generation ($1,568 to $8,765)
- Chairs, Phones, and Tables are the top-performing subcategories in sales and profit
- Some product subcategories appear to be operating at a loss or lower profitability
- Sales and profit trends show fluctuations throughout the year, suggesting potential seasonality or impact of specific initiatives

## 6. Recommendations
1. Implement targeted retention strategies to convert one-time buyers into repeat customers
2. Analyze characteristics of top customers to inform personalized marketing efforts
3. Consider expanding or promoting high-performing product categories (Chairs, Phones, Tables)
4. Review and potentially optimize or phase out underperforming product categories
5. Investigate sales and profit fluctuations to better predict and prepare for future trends
6. Develop strategies to increase average order value, building on the success in growing sales per customer

## Conclusion
This analysis provides valuable insights into the business's performance and customer behavior, offering a strong foundation for data-driven decision-making. By acting on these insights and recommendations, the company is well-positioned to continue its growth trajectory and enhance its market position.
