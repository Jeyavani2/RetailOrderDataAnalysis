#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import mysql.connector
import altair as alt
def get_db_connection():
    return mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "jeya",
    database="retailorder",        
    auth_plugin='mysql_native_password'

)
def Top_10_Highest_Revenue_Products():
    myconnection = get_db_connection()
    mycursor=myconnection.cursor()
    query="""select product_id,sum(sale_price*quantity) as revenue from product_data group by product_id order by revenue desc limit 10"""
    mycursor.execute(query)
    results = mycursor.fetchall()
    mycursor.close()
    myconnection.close()
    df = pd.DataFrame(results,columns=['Product','Revenue'])
    return df
def Top_5_Cities_Highest_Profit():
    myconnection = get_db_connection()
    mycursor=myconnection.cursor()
    query="""SELECT city, SUM(profit) AS total_profit FROM order_data,product_data
         where order_data.order_id = product_data.order_id GROUP BY city ORDER BY 
        total_profit DESC LIMIT 5"""
    mycursor.execute(query)
    results = mycursor.fetchall()
    mycursor.close()
    myconnection.close()
    df = pd.DataFrame(results, columns=["City", "Total Profit"])
    return df
def Total_Discount_given_foreach_category():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query="""SELECT category, SUM(discount) AS total_discount FROM order_data,product_data where
        order_data.order_id=product_data.order_id  GROUP BY category"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Category", "Total Discount"])
        return df   
def Average_sale_price_per_product():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query="""SELECT product_id,category, AVG(sale_price) AS avg_sale_price FROM order_data,product_data where  order_data.order_id = product_data.order_id          GROUP BY product_id, category"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Product","Category", "Average Sale Price"])
        return df
def Region_with_Highest_Average_Sale_Price():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query="""SELECT region, AVG(sale_price) AS avg_sale_price FROM order_data,
        product_data where order_data.order_id = product_data.order_id GROUP BY region 
        ORDER BY avg_sale_price DESC"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Region", "Average Sale Price"])
        return df 
def Total_Profit_Per_Category():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query="""SELECT category, SUM(profit) AS total_profit FROM 
        order_data,product_data where order_data.order_id = product_data.order_id GROUP BY category"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Category", "Total Profit"])
        return df    
def Top_3_Segments_with_Highest_Quantityof_Orders():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query="""SELECT segment, SUM(quantity) AS total_quantity FROM 
        order_data,product_data where order_data.order_id = product_data.order_id GROUP BY 
        segment ORDER BY total_quantity DESC LIMIT 3"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Segment", "Total Quantity"])
        return df   
def Average_Discount_Percentage_Given_per_Region():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query="""select region,avg(discount_percent) from order_data,product_data where order_data.order_id=product_data.order_id group by region;"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Region", "Average Discount Percentage"])
        return df 

def Product_Category_with_Highest_Total_Profit():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query="""SELECT product_id,category, sum(profit) AS total_profit FROM 
        order_data,product_data where order_data.order_id = product_data.order_id GROUP BY product_id,category ORDER BY 
        total_profit DESC """
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Product","Category", "Total Profit"])
        return df
def Total_Revenue_Generated_Per_Year():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query="""select year(order_date) as yearr,sum(sale_price*quantity) as revenue  from order_data,product_data where order_data.order_id=product_data.order_id group by yearr"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Year", "Total Revenue"])
        return df    
    
def Discount_grade_per_orders():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query="""SELECT CASE WHEN discount_percent >=5  THEN 'High Discount' 
        WHEN discount_percent BETWEEN 3 AND 4 THEN 'Medium Discount' 
        ELSE 'Low Discount' END AS discount_grade,count(order_id) as orders
		FROM product_data group by discount_grade order by orders desc"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results,columns=['Discount Grade','Orders'])
        return df   
def Monthly_Revenue_Trends():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query="""SELECT DATE_FORMAT(order_date, '%Y-%m') AS month, SUM(sale_price * quantity) AS monthly_revenue
        FROM product_data JOIN order_data ON product_data.order_id = order_data.order_id GROUP BY month ORDER BY month"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Month", "Monthly Revenue"])
        return df  
def Shipmode_Order_Trends():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query="""select ship_mode,count(order_id) from order_data group by ship_mode"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Ship Mode", "Orders"])
        return df  
def Yearly_Profit_Growth():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query="""SELECT YEAR(order_date) AS yearr, SUM(profit) AS yearly_profit FROM product_data 
        JOIN order_data ON product_data.order_id = order_data.order_id GROUP BY yearr ORDER BY yearr"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Year", "Yearly Profit"])
        return df   
def Category_with_the_highest_sale_price_in_Region():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        mycursor.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
        query="""select region,category,max(sale_price) as m from order_data,product_data where order_data.order_id=product_data.order_id group by region"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=[ "Region","Category","Maximum Sale Price"])
        return df    
def  Product_with_Loss_Margin():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        #mycursor.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
        query="""SELECT product_id, (SUM(profit) / SUM(sale_price * quantity)) * 100 AS margin
        FROM product_data GROUP BY product_id HAVING margin < 0 ORDER BY margin"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=[ "Product","Loss Margin"])
        return df 
def City_with_maximum_discount():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        mycursor.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
        query=""" select city,max(discount) from order_data,product_data where order_data.order_id=product_data.order_id"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["City","Maximum Discount"])
        return df
def Product_with_total_sale_price():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query=""" select product_id,sum(sale_price) as sale from product_data group by product_id order by sale desc """
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Product","Maximum Sale Price"])
        return df   
def Top_5_states_with_high_profit():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query=""" select state,sum(profit) as total from order_data join product_data on order_data.order_id=product_data.order_id  group by state order by total desc limit 5  """
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["State","Total Profit"])
        return df  
def Segment_with_Maximum_Discount():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor()
        query=""" select segment,max(discount) as discount  from order_data join product_data on order_data.order_id=product_data.order_id  group by segment order by  discount desc """
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Segment","Maximum Discount"])
        return df      
#------------------------------------------------------------------------------------------------------------------------------------------------------------    
intro = st.sidebar.radio('Main Menu',["Home page","Check In for Retail Order's Business Analysis","Advanced Analytics: Uncover Key Insights on Discounts, Sales, and Product Performance"])
if  intro=='Home page':
    # Project Introduction
    st.title("Retail Order Data Analysis")
    st.subheader("A Deep Dive into Sales Data Using Kaggle API, SQL, and Streamlit")

    st.write("""
        In this project, we aim to analyze and optimize sales performance by identifying key trends, top-performing products, 
        and growth opportunities using a dataset of sales transactions.
        Our goal is to gain insights that will guide decision-making and drive business growth.
    """)

    st.write("""
        **Tools & Technologies Used:**
        - **Kaggle API**: For fetching retail order data directly from Kaggle.
        - **Python**: Utilized for data extraction, cleaning, and analysis.
        - **SQL**: Used for storing and querying the cleaned dataset.
        - **Streamlit**: For building a real-time, interactive dashboard to display insights.
    """)
    # Project Objectives
    st.subheader("Project Objectives")
    st.write("""
        1. **Identify Top-Performing Products**: Analyze the products and categories that contribute the most to revenue and profit.
        2. **Sales Trend Analysis**: Perform Year-over-Year (YoY) and Month-over-Month (MoM) comparisons to identify sales trends.
        3. **Profit Margin Analysis**: Highlight the subcategories with the highest profit margins to guide future sales strategies.
        4. **Regional Analysis**: Explore sales data by region to identify high-performing areas.
        5. **Discount Impact**: Analyze the effects of discounts on product sales, especially those with more than 20 percentage off.
    """)

    st.write("""
        This project integrates multiple components:
        - **Data Extraction**: Using Kaggle API to fetch raw sales data.
        - **Data Cleaning**: Applying transformations to standardize and prepare the data for analysis.
        - **SQL Database Integration**: Moving the cleaned data into SQL for efficient querying and analysis.
        - **Business Insights**: Extracting valuable insights using SQL queries to answer key business questions.
        - **Interactive Dashboard**: Building a real-time dashboard in Streamlit to visualize the findings.
    """)
     # Tools Overview
    st.write("""
        **Streamlit Features for This Project:**
        
        1. **Real-Time Data Queries**: Query your SQL database and display live results directly in the Streamlit interface.
        2. **Dynamic Data Filtering**: Allow users to filter and analyze the dataset based on various criteria such as product category, region, and sales trends.
        3. **Visualization**: Use charts and tables to present insights visually for better interpretation and decision-making.
    """)

    # Brief Explanation of the Flow
    st.subheader("Project Flow")
    st.write("""
        1. **Data Extraction**: Fetch raw data from Kaggle API.
        2. **Data Cleaning**: Clean the dataset to handle missing values, standardize column names, and derive new columns (e.g., discount, sale price).
        3. **SQL Integration**: Load cleaned data into a SQL database for efficient querying.
        4. **SQL Queries**: Run advanced queries to generate business insights and trends.
        5. **Streamlit Dashboard**: Build an interactive dashboard to display the results dynamically.
    """)

    # Conclusion
    st.write("""
        This project aims to provide meaningful insights into the retail order data, helping businesses make informed decisions about 
        their products, sales strategies, and market opportunities. By combining data extraction, cleaning, SQL analysis, and Streamlit 
        visualization, we offer a comprehensive approach to analyzing retail sales performance.
    """)

    # Call to Action
    

elif intro == "Check In for Retail Order's Business Analysis":
    # Business Analysis Section
    st.title("Retail Order's Business Analysis")
    st.subheader("Dive deeper into the business analysis of retail orders.")
    st.write("""
        🚀 **Ready to explore the data?** Use the interactive tools below to filter and analyze the data in real-time!
    """)
    col1, col2= st.columns([3,5])

    with col1:
        st.markdown(' #### Unlock Insights: Explore Top Products, Categories, and Regional Trends')
    with col2:
        selected_question = st.selectbox("Select a Question to View Analysis", [
            "Find top 10 highest revenue generating products",
            "Find the top 5 cities with the highest profit margins",
            "Calculate the total discount given for each category",
            "Find the average sale price per product category",
            "Find the region with the highest average sale price",
            "Find the total profit per category",
            "Identify the top 3 segments with the highest quantity of orders",
            "Determine the average discount percentage given per region",
            "Find the product category with the highest total profit",
            "Calculate the total revenue generated per year"
             ],index=None)

        if selected_question == "Find top 10 highest revenue generating products":
        
              a=pd.DataFrame(Top_10_Highest_Revenue_Products())
              a.index.name="Index"
             
             
              st.write(a)
        
              chart_container = st.container()
              with chart_container:
             
                  fig, ax = plt.subplots(1,1,figsize=(50,50),dpi=50)
                  ax.bar(a['Product'],a['Revenue'],color='#00FFFF')
              

# Set text color
            
                  ax.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
                  ax.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
                  ax.set_xlabel('Products', color='#000080',fontsize=180)
                  ax.set_ylabel('Revenue', color='#000080',fontsize=180)
           
                  st.header(':rainbow[Bar chart for Top_10_Highest_Revenue_Products]')
           

# Display the chart in Streamlit
                  st.pyplot(fig)
                  #plt.clf()
                  fig.clear()
                  ax.clear()
                  plt.close(fig)
                  del fig
                  figp, axp = plt.subplots()
                  
                  axp.pie(a['Revenue'],labels=a['Product'],autopct='%1.1f%%',startangle=90)
                  
                  axp.axis('equal')
                  st.write('---')
                  st.header(':rainbow[Pie chart for Top_10_Highest_Revenue_Products]')
                  st.pyplot(figp) 
                  figp.clear()
                  axp.clear()
                  plt.close(figp)
                  del figp
       
        elif selected_question == "Find the top 5 cities with the highest profit margins":
                a=pd.DataFrame(Top_5_Cities_Highest_Profit())
                a.index.name="Index"
                st.write(a)
                b=pd.DataFrame(a.loc[:,['City','Total Profit']])
                chart_container = st.container()
                with chart_container:
              
                  fig1, ax1 = plt.subplots(1,1,figsize=(50,50))
                  ax1.bar(a['City'],a['Total Profit'],color='#FFFF00')
            

# Set text color
              #ax.set_facecolor='yellow' 
                  ax1.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
                  ax1.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
                  ax1.set_xlabel('City', color='#000080',fontsize=180)
                  ax1.set_ylabel('Total Profit', color='#000080',fontsize=180)
             
                  st.header(':rainbow[Bar chart for Top 5 cities with the highest profit margins]')

# Display the chart in Streamlit
                  st.pyplot(fig1)
                  fig1.clear()
                  ax1.clear()
                  plt.close(fig1)
                  del fig1
        elif selected_question == "Calculate the total discount given for each category":
                  a=pd.DataFrame(Total_Discount_given_foreach_category())
                  a.index.name="Index"
                  st.write(a)
                  b=pd.DataFrame(a.loc[:,['Category','Total Discount']])
                  chart_container = st.container()
                  with chart_container:
                  
                       fig2, ax2 = plt.subplots(1,1,figsize=(50,50))
                       ax2.bar(a['Category'],a['Total Discount'],color='#FFFF00')
                       ax2.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
                       ax2.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
                       ax2.set_xlabel('Category', color='#000080',fontsize=180)
                       ax2.set_ylabel('Total Discount', color='#000080',fontsize=180)
                 
            
# Display the chart in Streamlit
                       st.header(':rainbow[Bar chart for Total discount given for each category]')
                       st.pyplot(fig2)
                       fig2.clear()
                       ax2.clear()
                       plt.close(fig2)
                       del fig2
                 
                       figp1, axp1 = plt.subplots()
                   
                       axp1.pie(b['Total Discount'],labels=b['Category'],autopct='%1.1f%%',startangle=90)
                  
                       axp1.axis('equal')
                       st.write('---')
                       st.header(':rainbow[Pie chart for Total discount given for each category]')
                       st.pyplot(figp1)  
                       figp1.clear()
                       axp1.clear()
                       plt.close(figp1)
                       del figp1
        elif selected_question == "Find the average sale price per product category":
                       a=pd.DataFrame(Average_sale_price_per_product())
                       a.index.name="Index" 
                       st.write(a)
        
                       chart_container = st.container()
                       with chart_container:
                       

                       
                            chartt = alt.Chart(a).mark_bar().encode(
                            x=alt.X('Product', title='Product'),
                            y=alt.Y('Average Sale Price', title='Average Sale Price'),
                            color='Product',
                            column=alt.Column('Category', header=alt.Header(titleOrient='bottom'))).properties( title='Grouped Bar Chart')
                            st.altair_chart(chartt)
        elif selected_question == "Find the region with the highest average sale price":
                       a=pd.DataFrame(Region_with_Highest_Average_Sale_Price())
                       a.index.name="Index" 
                       st.write(a) 
        
                       chart_container = st.container()
                       with chart_container:
                            fig3, ax3 = plt.subplots(1,1,figsize=(50,50))
                            ax3.bar(a['Region'],a['Average Sale Price'],color='#7fffd4')
                            ax3.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
                            ax3.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
                            ax3.set_xlabel('Region', color='#000080',fontsize=180)
                            ax3.set_ylabel('Average Sale Price', color='#000080',fontsize=180)
                  
# Display the chart in Streamlit
                            st.header(':rainbow[Bar chart for Region with Highest Average Sale Price]')
                            st.pyplot(fig3)
                            fig3.clear()
                            ax3.clear()
                            plt.close(fig3)
                            del fig3
# Display the chart in Streamlit
                        
        elif selected_question == "Find the total profit per category":
                a=pd.DataFrame(Total_Profit_Per_Category())
                a.index.name="Index"
                st.write(a) 
                chart_container = st.container()
                with chart_container:
                       fig4, ax4 = plt.subplots(1,1,figsize=(50,50))
                       ax4.bar(a['Category'],a['Total Profit'],color='#CCCCFF')
                       ax4.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
                       ax4.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
                       ax4.set_xlabel('Category', color='#000080',fontsize=180)
                       ax4.set_ylabel('Total Profit', color='#000080',fontsize=180)
                  
            
# Display the chart in Streamlit
                       st.header(':rainbow[Bar chart for Total Profit Per Category]')
                       st.pyplot(fig4)
                       fig4.clear()
                       ax4.clear()
                       plt.close(fig4)
                       del fig4  
                       figp2, axp2= plt.subplots()
                   
                       axp2.pie(a['Total Profit'],labels=a['Category'],autopct='%1.1f%%',startangle=90)
                 
                       axp2.axis('equal')
                       st.write('---')
                       st.header(':rainbow[Pie chart for Total Profit Per Category]')
                       st.pyplot(figp2)  # Equal aspect ratio ensures that pie is drawn as a circle
                       figp2.clear()
                       axp2.clear()
                       plt.close(figp2)
                       del figp2
        elif selected_question == "Identify the top 3 segments with the highest quantity of orders":
            
                a=pd.DataFrame(Top_3_Segments_with_Highest_Quantityof_Orders())
                a.index.name="Index"
                st.write(a) 
                chart_container = st.container()
                with chart_container:
                       fig5, ax5 = plt.subplots(1,1,figsize=(50,50))
                       ax5.bar(a['Segment'],a['Total Quantity'],color='#800080')
                       ax5.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
                       ax5.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
                       ax5.set_xlabel('Segment', color='#000080',fontsize=180)
                       ax5.set_ylabel('Total Quantity', color='#000080',fontsize=180)
                   
            
# Display the chart in Streamlit
                       st.header(':rainbow[Bar chart for Top 3 Segments with Highest Quantityof Orders]')
                       st.pyplot(fig5)
                       fig5.clear()
                       ax5.clear()
                       plt.close(fig5)
                       del fig5
                       figp3, axp3 = plt.subplots()
                   
                       axp3.pie(a['Total Quantity'],labels=a['Segment'],autopct='%1.1f%%',startangle=90)
                  
                       axp3.axis('equal')
                       st.write('---')
                       st.header(':rainbow[Pie chart for Top 3 Segments with Highest Quantityof Orders]')
                       st.pyplot(figp3)  # Equal aspect ratio ensures that pie is drawn as a circle
                       figp3.clear()
                       axp3.clear()
                       plt.close(figp3)
                       del figp3
        elif selected_question == "Determine the average discount percentage given per region":
                a=pd.DataFrame(Average_Discount_Percentage_Given_per_Region())
                a.index.name="Index"
                st.write(a)
                chart_container = st.container()
                with chart_container:
                       fig6, ax6 = plt.subplots(1,1,figsize=(50,50))
                       ax6.bar(a['Region'],a['Average Discount Percentage'],color='#ffcba4')
                       ax6.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
                       ax6.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
                       ax6.set_xlabel('Region', color='#000080',fontsize=180)
                       ax6.set_ylabel('Average Discount Percentage', color='#000080',fontsize=180)
                 
            
# Display the chart in Streamlit
                       st.header(':rainbow[Bar chart for Average Discount Percentage Given per Region]')
                       st.pyplot(fig6)
                       fig6.clear()
                       ax6.clear()
                       plt.close(fig6)
                       del fig6  
        elif selected_question == "Find the product category with the highest total profit":
                a=pd.DataFrame(Product_Category_with_Highest_Total_Profit())
                a.index.name="Index" 
                st.write(a)
                chart_container = st.container()
                with chart_container:
                       chartt = alt.Chart(a).mark_bar().encode(
                       x=alt.X('Product', title='Product'),
                       y=alt.Y('Total Profit', title='Total Profit'),
                       color='Product',
                       column=alt.Column('Category', header=alt.Header(titleOrient='bottom'))).properties( title='Grouped Bar Chart')
                       st.altair_chart(chartt)
        elif selected_question == "Calculate the total revenue generated per year":
                a=pd.DataFrame(Total_Revenue_Generated_Per_Year()) 
                a.index.name="Index"
                st.write(a)
                chart_container = st.container()
                with chart_container:
                       figp4, axp4 = plt.subplots()
                   
                       axp4.pie(a['Total Revenue'],labels=a['Year'],autopct='%1.1f%%',startangle=90)
                   
                       axp4.axis('equal')
                       st.write('---')
                       st.header(':rainbow[Pie chart for Total_Revenue_Generated_Per_Year]')
                       st.pyplot(figp4) 
                       figp4.clear()
                       axp4.clear()
                       plt.close(figp4)
                       del figp4
else : 
    st.title("Retail Order Data Analysis") 
    st.subheader("Dive deeper into the business analysis of retail orders.")
    st.write("""
        🚀 **Ready to explore the data?** Use the interactive tools below to filter and analyze the data in real-time!
    """)
    colA, colB= st.columns([3,5])
     
    with colA:
        st.markdown(' #### Advanced Analytics: Uncover Key Insights on Discounts, Sales, and Product Performance')
    with colB:
        own_selected_question = st.selectbox("Select a Question to View Analysis", [
            "Find Discount Grade Per Orders",
            "Find the Monthly Revenue Trends",
            "Calculate the Ship Mode Wise Order Analysis",
            "Calculate the Yearly Profit Growth",
            "Find the Category with the highest sale price in Region",
            "Find the Product with Loss Margin",
            "Find the City with high Discount",
            "Find the Product with total Sale Price",
            "Find the top 5 States with high Profit",
            "Find the Segment with Maximum Discount"]
        ,index=None)

        if own_selected_question == "Find Discount Grade Per Orders":
                a=pd.DataFrame(Discount_grade_per_orders())
                a.index.name="Index"
                st.write(a)
                chart_container = st.container()
                with chart_container:
                       figp, axp = plt.subplots()
                   
                       axp.pie(a['Orders'],labels=a['Discount Grade'],autopct='%1.1f%%',startangle=90)
                   
                       axp.axis('equal')
                       st.write('---')
                       st.header(':rainbow[Pie chart for Discount_grade_per_orders]')
                       st.pyplot(figp) 
                       figp.clear()
                       axp.clear()
                       plt.close(figp)
                       del figp
                
        elif own_selected_question == "Find the Monthly Revenue Trends":
               a=pd.DataFrame(Monthly_Revenue_Trends()) 
               a.index.name="Index"
               st.write(a)
               chart_container = st.container()
               with chart_container:
                       fig, ax = plt.subplots(1,1,figsize=(50,50))
                       ax.bar(a['Month'],a['Monthly Revenue'],color='#800080')
                       ax.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
                       ax.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
                       ax.set_xlabel('Month', color='#000080',fontsize=180)
                       ax.set_ylabel('Monthly Revenue', color='#000080',fontsize=180)
                  
            
# Display the chart in Streamlit
                       st.header(':rainbow[Bar chart for Monthly Revenue Trends]')
                       st.pyplot(fig)
                       fig.clear()
                       ax.clear()
                       plt.close(fig)
                       del fig
        elif own_selected_question == "Calculate the Ship Mode Wise Order Analysis":
               a=pd.DataFrame(Shipmode_Order_Trends()) 
               a.index.name="Index"
               st.write(a)
               chart_container = st.container()
               with chart_container:
                       fig, ax = plt.subplots(1,1,figsize=(50,50))
                       ax.bar(a['Ship Mode'],a['Orders'],color='#800080')
                       ax.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
                       ax.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
                       ax.set_xlabel('Ship Mode', color='#000080',fontsize=180)
                       ax.set_ylabel('Orders', color='#000080',fontsize=180)
                   #ax.set_title('Total discount given for each category',color='#aa423a',fontsize=150)
            
# Display the chart in Streamlit
                       st.header(':rainbow[Bar chart for Ship Mode Wise Order Analysis]')
                       st.pyplot(fig)
                       fig.clear()
                       ax.clear()
                       plt.close(fig)
                       del fig 
                   
        elif own_selected_question == "Calculate the Yearly Profit Growth":
               a=pd.DataFrame(Yearly_Profit_Growth()) 
               a.index.name="Index"
               st.write(a)
               chart_container = st.container()
               with chart_container:
                       fig, ax = plt.subplots(1,1,figsize=(50,50))
                       ax.bar(a['Year'],a['Yearly Profit'],color='#800080')
                       ax.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
                       ax.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
                       ax.set_xlabel('Year', color='#000080',fontsize=180)
                       ax.set_ylabel('Yearly Profit', color='#000080',fontsize=180)
                  
            
# Display the chart in Streamlit
                       st.header(':rainbow[Bar chart for Yearly Profit Growth]')
                       st.pyplot(fig)
                       fig.clear()
                       ax.clear()
                       plt.close(fig)
                       del fig 
        elif own_selected_question == "Find the Category with the highest sale price in Region":
               a=pd.DataFrame(Category_with_the_highest_sale_price_in_Region()) 
               a.index.name="Index"
               st.write(a)
        elif own_selected_question == "Find the Product with Loss Margin":
               a=pd.DataFrame(Product_with_Loss_Margin()) 
               a.index.name="Index"
               st.write(a)
        elif own_selected_question == "Find the City with high Discount":
               a=pd.DataFrame(City_with_maximum_discount())   
               a.index.name="Index"
               st.write(a)
        elif own_selected_question == "Find the Product with total Sale Price":
               a=pd.DataFrame(Product_with_total_sale_price())   
               a.index.name="Index"
               st.write(a)    
               chart_container = st.container()
               with chart_container:
                       fig, ax = plt.subplots(1,1,figsize=(50,50))
                       ax.bar(a['Product'],a['Maximum Sale Price'],color='#800080')
                       ax.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
                       ax.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
                       ax.set_xlabel('Product', color='#000080',fontsize=180)
                       ax.set_ylabel('Maximum Sale Price', color='#000080',fontsize=180)
                  
            
# Display the chart in Streamlit
                       st.header(':rainbow[Bar chart for Product with Total Sale Price]')
                       st.pyplot(fig)
                       fig.clear()
                       ax.clear()
                       plt.close(fig)
                       del fig     
        elif own_selected_question == "Find the top 5 States with high Profit":
               a=pd.DataFrame(Top_5_states_with_high_profit())   
               a.index.name="Index"
               st.write(a)    
               chart_container = st.container()
               with chart_container:
                       fig, ax = plt.subplots(1,1,figsize=(50,50))
                       ax.bar(a['State'],a['Total Profit'],color='#800080')
                       ax.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
                       ax.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
                       ax.set_xlabel('State', color='#000080',fontsize=180)
                       ax.set_ylabel('Total Profit', color='#000080',fontsize=180)
                  
            
# Display the chart in Streamlit
                       st.header(':rainbow[Bar chart for Top 5 States with high Profit]')
                       st.pyplot(fig)
                       fig.clear()
                       ax.clear()
                       plt.close(fig)
                       del fig 
        elif own_selected_question == "Find the Segment with Maximum Discount":
               a=pd.DataFrame(Segment_with_Maximum_Discount())   
               a.index.name="Index"
               st.write(a)    
               chart_container = st.container()
               with chart_container:
                       fig, ax = plt.subplots(1,1,figsize=(50,50))
                       ax.bar(a['Segment'],a['Maximum Discount'],color='#ffd700')
                       ax.tick_params(axis='x', colors='red',labelrotation=90,labelsize=100)
                       ax.tick_params(axis='y', colors='#008000',labelsize=100)

# Customize other chart elements as needed (e.g., labels, title)
                       ax.set_xlabel('Segment', color='#000080',fontsize=180)
                       ax.set_ylabel('Maximum Discount', color='#000080',fontsize=180)
                  
            
# Display the chart in Streamlit
                       st.header(':rainbow[Bar chart for Segments with high Profit]')
                       st.pyplot(fig)
                       fig.clear()
                       ax.clear()
                       plt.close(fig)
                       del fig               
                      
        
