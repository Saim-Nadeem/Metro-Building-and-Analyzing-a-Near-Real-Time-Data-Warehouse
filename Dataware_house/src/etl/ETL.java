package etl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.io.*;
import java.text.SimpleDateFormat;

public class ETL {

	//transition_data object
	public static class transactions_data{
		int Order_ID;
		Date Order_Date;
		int ProductID;
		int QuantityOrdered;
		int customer_id;
		int time_id;
		
		//constructor
		public transactions_data(int a,Date b,int c,int d,int e,int f){
			Order_ID=a;
			Order_Date=b;
			ProductID=c;
			QuantityOrdered=d;
			customer_id=e;
			time_id=f;
		}	
	}

	//customer_data object
	public static class customers_data{
		int customer_id;
	    String customer_name;
	    String gender;
	    
	    //constructor
	    public customers_data(int a,String b,String c) {
	    	customer_id=a;
	    	customer_name=b;
	    	gender=c;
	    }
	}
	
	//product_data object
	public static class products_data{
		int productID;
	    String productName;
	    float productPrice;
	    int supplierID;
	    String supplierName;
	    int storeID;
	    String storeName;
	    
	    //constructor
	    public products_data(int a,String b,float c,int d,String e,int f,String g) {
	    	productID=a;
		    productName=b;
		    productPrice=c;
		    supplierID=d;
		    supplierName=e;
		    storeID=f;
		    storeName=g;
	    }
	}

	//transformend_data object
	public static class transformend_data{
		int Order_ID;
		Date Order_Date;
		int customer_id;
	    String customer_name;
	    String gender;
	    int QuantityOrdered;
	    int productID;
	    String productName;
	    float productPrice;
	    int supplierID;
	    String supplierName;
	    int storeID;
	    String storeName;
	    float sale;
	    int time_id;
	    
	    public transformend_data(int a,Date b,int c,String d,String e,int f, int g,String h,float i,int j,String k, int l,String m,int n,int o) {
	    	Order_ID=a;
			Order_Date=b;
			customer_id=c;
		    customer_name=d;
		    gender=e;
		    QuantityOrdered=f;
		    productID=g;
		    productName=h;
		    productPrice=i;
		    supplierID=j;
		    supplierName=k;
		    storeID=l;
		    storeName=m;
		    sale=n;
		    time_id=o;
	    }
	}
	
	static Queue<List<transactions_data>> stream_buffer = new LinkedList<>();
	static List<customers_data> MD_customer=new ArrayList<>();
	static List<products_data> MD_product=new ArrayList<>();
	static Map<Integer, transformend_data> hashtable=new HashMap<>();
	static int partition=1;
	static String MD_database="meta_data";
	static String MD_username="root";
	static String MD_password="Phone321";
	static String DW_database="data_warehouse";
	static String DW_username="root";
	static String DW_password="Phone321";
	
	
	//thread 1
	public class Read_Transition_Data extends Thread {
	    public void run() {
	        BufferedReader reader=null;
	        String line="";
	        List<transactions_data> chunk=new ArrayList<>();
	        int saim=0;
	        
	        try {
	            reader = new BufferedReader(new FileReader("src\\transactions.csv"));
	            reader.readLine();

	            SimpleDateFormat j=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	            while ((line=reader.readLine())!= null) {
	                String[] r = line.split(",");
	                String saim2=saim+r[0];
	                transactions_data k=new transactions_data(Integer.parseInt(saim2),j.parse(r[1]),Integer.parseInt(r[2]),Integer.parseInt(r[3]),Integer.parseInt(r[4]),Integer.parseInt(r[5]));
	                chunk.add(k);
	                saim=(saim%9)+1;
	                
	                if (chunk.size()==10) {
	                    synchronized (stream_buffer) {
	                        stream_buffer.add(new ArrayList<>(chunk));
	                        chunk.clear();
	                        stream_buffer.notify();
	                    }
	                }
	            }

	            if (!chunk.isEmpty()) {
	                synchronized (stream_buffer) {
	                    stream_buffer.add(new ArrayList<>(chunk));
	                    stream_buffer.notify();
	                }
	            }

	            synchronized (stream_buffer) {
	                stream_buffer.add(Collections.singletonList(new transactions_data(-1, null, -1, -1, -1,-1)));
	                stream_buffer.notify();
	            }

	            reader.close();
	        }
	        catch(Exception e){
	            
	        }
	    }
	}

	//thread 2
	public class mesh_join extends Thread {
		Queue<List<transactions_data>> queue_chunks=new LinkedList<>();
	    public void run() {
	        try {
	        	Class.forName("com.mysql.cj.jdbc.Driver");
		    	Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/"+DW_database,DW_username,DW_password);
	            while (true) {
	                List<transactions_data> chunk;

	                synchronized (stream_buffer) {
	                    while (stream_buffer.isEmpty()) {
	                        stream_buffer.wait();
	                    }
	                    chunk=(List<transactions_data>)stream_buffer.poll();
	                }

	                if (chunk.size()==1&&chunk.get(0).Order_ID==-1) {
	                    break;
	                }
	                
	                queue_chunks.add(chunk);
	                add_to_hashtable(chunk);
	                
	                //join
	                read_disk_partion("customers",partition);
	                read_disk_partion("products", partition);
	                join_partition();
	                
	                partition=(partition%4)+1;
	                //remove
	                if (queue_chunks.size()>=4) {
                        List<transactions_data> temp=queue_chunks.poll();
                        for(transactions_data i:temp) {
                        	Data_to_datawarehouse(hashtable.get(i.Order_ID),con);
                            hashtable.remove(i.Order_ID);
                        }
                    }
	                
	            }
	            

                //remaining chunks
	            partition=1;
	            while(partition!=5) {
	            	//join
	                read_disk_partion("customers",partition);
	                read_disk_partion("products", partition);
	                join_partition();
	                
	                if(partition==4) {
	                	while(!queue_chunks.isEmpty()) {
	                		//remove
			                List<transactions_data> temp=queue_chunks.poll();
		                    for(transactions_data i:temp) {
		                    	Data_to_datawarehouse(hashtable.get(i.Order_ID),con);
		                        hashtable.remove(i.Order_ID);
		                    }
	                	}
	                }
	                
	                
	                partition++;
	                
	            }
	            
	            System.out.println("Processed Finished");
	        } 
	        catch(Exception e){
	            
	        }
	    }
	}
	
	//function to add data to Data warehouse
	public static void Data_to_datawarehouse(transformend_data t_d,Connection con) {
		try {	
			if(t_d==null) {
				return;
			}
	    	con.setAutoCommit(false);
	    	
	    	//customer table
	    	try {
				if((check_ID(con,"select 1 from customers where customer_id=?",t_d.customer_id)==false)) {
					PreparedStatement temp=con.prepareStatement("insert into customers values(?,?,?)");
					temp.setInt(1,t_d.customer_id);
					temp.setString(2,t_d.customer_name);
					temp.setString(3,t_d.gender);
					temp.executeUpdate();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
	    	
	    	//product table
	    	try {
				if((check_ID(con,"select 1 from products where productID=?",t_d.productID)==false)) {
					PreparedStatement temp=con.prepareStatement("insert into products values(?,?,?)");
					temp.setInt(1,t_d.productID);
					temp.setString(2,t_d.productName);
					temp.setFloat(3,t_d.productPrice);
					temp.executeUpdate();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
	    	
	    	
	    	//store table
	    	try {
				if((check_ID(con,"select 1 from store where storeID=?",t_d.storeID)==false)) {
					PreparedStatement temp=con.prepareStatement("insert into store values(?,?)");
					temp.setInt(1,t_d.storeID);
					temp.setString(2,t_d.storeName);
					temp.executeUpdate();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
	    	
	    	
	    	//supplier table
	    	try {
				if((check_ID(con,"select 1 from supplier where supplierID=?",t_d.supplierID)==false)) {
					PreparedStatement temp=con.prepareStatement("insert into supplier values(?,?)");
					temp.setInt(1,t_d.supplierID);
					temp.setString(2,t_d.supplierName);
					temp.executeUpdate();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
	    	
	    	
	    	//time table
	    	int time_id=0;
	    	try {
	    		SimpleDateFormat day_f=new SimpleDateFormat("d");
	    	    SimpleDateFormat m_f=new SimpleDateFormat("M");
	    	    SimpleDateFormat y_f=new SimpleDateFormat("yyyy");
	    	    SimpleDateFormat d_f=new SimpleDateFormat("yyyy-MM-dd");
	    	    SimpleDateFormat d_w_f=new SimpleDateFormat("u");

	    	    java.util.Date utilDate = t_d.Order_Date;
	    	    int d=Integer.parseInt(day_f.format(utilDate));
	    	    int m=Integer.parseInt(m_f.format(utilDate));
	    	    int y=Integer.parseInt(y_f.format(utilDate));
	    	    String date1=d_f.format(utilDate);

	    	    String q="Q"+((m-1)/3+1);
	    	    int h_y=(m<=6)?1:2;
	    	    int w=(Integer.parseInt(d_w_f.format(utilDate))>=6)?1:0;
	    	    time_id=y*10000+m*100+d;
				if((check_ID(con,"select 1 from time where time_id=?",time_id)==false)) {
					PreparedStatement temp=con.prepareStatement("insert into time values(?,?,?,?,?,?,?,?)");
					temp.setInt(1,time_id);
					temp.setDate(2,java.sql.Date.valueOf(date1));
					temp.setInt(3,d);
					temp.setInt(4,m);
					temp.setString(5,q);
					temp.setInt(6,y);
					temp.setInt(7,w);
					temp.setInt(8,h_y);
					temp.executeUpdate();
				}
			} 
	    	catch (Exception e) {
				e.printStackTrace();
			}
	    	
	    	
	    	//fact table sales
	    	try {
	    		PreparedStatement temp=con.prepareStatement("insert into sales values(?,?,?,?,?,?,?,?,?)");
				temp.setInt(1,t_d.Order_ID);
				temp.setFloat(2,t_d.sale);
				temp.setInt(3,t_d.QuantityOrdered);
				temp.setInt(4,t_d.customer_id);
				temp.setInt(5,t_d.productID);
				temp.setInt(6,t_d.storeID);
				temp.setInt(7,t_d.supplierID);
				temp.setInt(8,time_id);
				temp.setInt(9,t_d.Order_ID%((int) Math.pow(10,((int) Math.log10(t_d.Order_ID)+1)- 1)));
				temp.executeUpdate();
			} catch (Exception e) {
				con.rollback();
				e.printStackTrace();
			}
	    	
	    	
	    	con.commit();
		}
    	catch(SQLException e2) {
    		System.out.println("SQLexecption "+e2);
    	}
	}
	
	//function to check if id present is dimension tables
	public static boolean check_ID(Connection con,String q,int id) {
		try {
			PreparedStatement stmt=con.prepareStatement(q);
            stmt.setInt(1,id);
            ResultSet result_subset=stmt.executeQuery();
            return result_subset.next();
		}
		catch(SQLException e2) {
			System.out.println("SQLexecption "+e2);
		}
		return false;
	}
	
	//function to join
	public void join_partition() {
        for(transformend_data i:hashtable.values()) {
            if (i.customer_id>0) {
                customers_data i2=check_customer(i.customer_id);
                if (i2!=null) {
                    i.customer_name=i2.customer_name;
                    i.gender=i2.gender;
                }
            }

            if (i.productID>0) {
                products_data i3=check_product(i.productID);
                if (i3!= null) {
                    i.productName=i3.productName;
                    i.productPrice=i3.productPrice;
                    i.supplierID=i3.supplierID;
                    i.supplierName=i3.supplierName;
                    i.storeID=i3.storeID;
                    i.storeName=i3.storeName;
                    i.sale=(int)(i.QuantityOrdered*i3.productPrice);
                }
            }
        }
    }

	// function to check if customer id present in current partition
    public customers_data check_customer(int c_ID) {
        for(customers_data i:MD_customer) {
            if (i.customer_id==c_ID) {
                return i;
            }
        }
        return null;
    }

    // function to check if product id present in current partition
    public products_data check_product(int p_ID) {
        for(products_data i:MD_product) {
            if (i.productID==p_ID) {
                return i;
            }
        }
        return null;
    }

	//function to hash chunk to hash table
	public static void add_to_hashtable(List<transactions_data> chunk) {
		for (transactions_data i:chunk) {
			transformend_data k=new transformend_data(i.Order_ID,i.Order_Date,i.customer_id,null,null,i.QuantityOrdered,i.ProductID,null,0.0f,-1,null,-1,null,0,i.time_id);
        	hashtable.put(i.Order_ID,k);
		}
    }
	
	//function to read Master data partition
	public static void read_disk_partion(String table_name,int p_no){

        	try {
        		int limit=0;
        		int offset=0;
        		Class.forName("com.mysql.cj.jdbc.Driver");
            	Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/"+MD_database,MD_username,MD_password);
            	
            	String sql1="select count(*) as t_r from "+table_name;
            	PreparedStatement stmt1=con.prepareStatement(sql1);
            	ResultSet result_t_r=stmt1.executeQuery(sql1);
            	result_t_r.next();
            	int t_r=result_t_r.getInt("t_r");
            	
            	result_t_r.close();
            	stmt1.close();
            	
            	limit=t_r/4;
            	offset=(p_no-1)*limit;
            	
            	if(p_no==4) {
            		limit=t_r-offset;
            	}
            	
            	String sql="select * from "+table_name+" limit ? offset ?";
            	PreparedStatement stmt=con.prepareStatement(sql);
                stmt.setInt(1, limit);
                stmt.setInt(2, offset);
                ResultSet result_subset=stmt.executeQuery();
                
                if(table_name.equals("products")) {
                	//products
                	if(!MD_product.isEmpty()) {
                		MD_product.clear();
                	}
                	
                	while(result_subset.next()) {
                		MD_product.add(new products_data(result_subset.getInt("productID"),result_subset.getString("productName"),result_subset.getFloat("productPrice"),result_subset.getInt("supplierID"),result_subset.getString("supplierName"),result_subset.getInt("storeID"),result_subset.getString("storeName")));
                    }
                }
                else {
                	//customer
                	if(!MD_customer.isEmpty()) {
                		MD_customer.clear();
                	}
                	
                	while(result_subset.next()) {
                		MD_customer.add(new customers_data(result_subset.getInt("customer_id"),result_subset.getString("customer_name"),result_subset.getString("gender")));
                    }
                }
                
                result_subset.close();
                stmt.close();
                con.close();
        	}
        	catch(ClassNotFoundException e1) {
        		System.out.println("JDBC driver not found "+e1);
        	}
        	catch(SQLException e2) {
        		System.out.println("SQLexecption "+e2);
        	}

	}
	
	//main
 	public static void main(String[] args) {
		
		String check="0";
		Scanner cin=new Scanner(System.in);
		
		System.out.println("Press 1 to use default SQL credentials for Master Data or press 2 to take input");
		check=cin.nextLine();
		
		if(check.equals("1")) {
			
		}
		else {
			System.out.println("Enter database name for master data = ");
			MD_database=cin.nextLine();
			
			System.out.println("Enter database username for master data = ");
			MD_username=cin.nextLine();
			
			System.out.println("Enter database Password for master data = ");
			MD_password=cin.nextLine();
		}
		
		System.out.println();
		check="0";
		System.out.println("Press 1 to use default SQL credentials for Data warehouse or press 2 to take input");
		check=cin.nextLine();
		
		if(check.equals("1")) {
			
		}
		else {
			System.out.println("Enter database name for data warehouse = ");
			DW_database=cin.nextLine();
			
			System.out.println("Enter database username for data warehouse = ");
			DW_username=cin.nextLine();
			
			System.out.println("Enter database Password for data warehouse = ");
			DW_password=cin.nextLine();
		}
		
		cin.close();
		
		System.out.println("Processed Started");
		ETL etl = new ETL();
        Read_Transition_Data readDataThread = etl.new Read_Transition_Data();
        mesh_join meshjoinThread =etl.new mesh_join();
        
        
        readDataThread.start();
        meshjoinThread.start();
        
	}

}