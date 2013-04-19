/**
 * realODMatrix main.java.realODMatrix.utility PostgreSQLConnector.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-4-16 上午11:11:14
 * email: gh.chen@siat.ac.cn
 */
package storm.realTraffic.bolt;

/**
 * realODMatrix main.java.realODMatrix.utility PostgreSQLConnector.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-4-16 上午11:11:14
 * email: gh.chen@siat.ac.cn
 *
 */

import org.geotools.xml.xsi.XSISimpleTypes.Int;
import  org.postgresql. * ;

import  java.sql. * ;

public   class  PostgreSQLConnector  {
	 private Connection conn=null;
	   private Statement st=null;
	   private ResultSet rs=null;
	   
	   public PostgreSQLConnector(String host, String port,String databaseName,String userName,String password){
	       try{
	           //写入驱动所在处，打开驱动
	           Class.forName("com.mysql.jdbc.Driver").newInstance();
		   //数据库，用户，密码，创建与具体数据库的连接
	           //conn=DriverManager.getConnection("jdbc:mysql://172.20.36.247:3306/"+databaseName,userName,password);
	       conn=DriverManager.getConnection("jdbc:postgresql://"+host+":"+port+"/"+databaseName,userName,password);
	           //创建执行sql语句的对象
		   st=conn.createStatement();
	          
	       }catch(Exception e){
	           System.out.println("连接失败"+e.toString());
	          
	       }       
	   }
	   
	  
	   public String select(String sqlStatement){
	       String result=new String();
	       int size=0;
	       try{
	           rs=st.executeQuery(sqlStatement);
	           size=st.getResultSet().getMetaData().getColumnCount();
	           while(rs!=null && rs.next()){
	        	   for(int i=0;i<size;i++){
	        		   result=result+rs.getString(i+1)+",";
	        	   }
	        	   result=result+"\n";
	                //result=rs.getString(n);
	                //列的记数是从1开始的，这个适配器和C#的不同
	           }
	           
	           rs.close();
	           return result;
	       }catch(Exception e){
	           System.out.println("查询失败"+e.toString());
	           
	           return null;
	       }
	   }
	   
	   public int query(String sqlStatement){
	       int row=0;
	       try{
	           row=st.executeUpdate(sqlStatement);
	           //this.close();
	           return row;
	       }catch(Exception e){
	           System.out.println("执行sql语句失败"+e.toString());
	           //this.close();
	           return row;
	       }
	   }
	   
	   public void close(){
	      try{
	          if(rs!=null)
	            this.rs.close();
	          if(st!=null)
	            this.st.close();
	          if(conn!=null)
	            this.conn.close();
	          
	      }catch(Exception e){
	          System.out.println("关闭数据库连接失败"+e.toString());
	      }
	   }
	      
    
    public   static   void  main(String args[])
    {
       PostgreSQLConnector postsql=new PostgreSQLConnector("172.20.20.161","5432","postgis_template", "postgres", "postgres");
 	   String s=postsql.select("select * from news limit 5;");
 	   
 	   System.out.println(s); 
 	   int count=234;
 	   String districtID="1020";
 	   
 	   
 	   int  p=postsql.query("update news set count= "+count+"where tazid="+districtID+" ;");
 	  System.out.println(p); 
    }
       /*System.out.print( " this is a test " );
        try 
         {
           Class.forName( " org.postgresql.Driver " ).newInstance();
          // String url = " jdbc:postgresql://localhost:5432/postgres " ;
           String url = " jdbc:postgresql://172.20.15.161:5432/postgres " ;
           Connection con = DriverManager.getConnection(url, " postgres " , " postgres " );
           Statement st = con.createStatement();
           String sql = " select * from testtable " ;
           ResultSet rs = st.executeQuery(sql);
            while (rs.next())
            {
               System.out.print(rs.getInt( 1 ));
               System.out.println(rs.getString( 2 ));
           } 
           rs.close();
           st.close();
           con.close();
           

       } 
        catch (Exception ee)
        {
           System.out.print(ee.getMessage());
       } 
   }    */
    
}
