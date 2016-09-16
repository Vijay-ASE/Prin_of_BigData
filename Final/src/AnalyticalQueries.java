import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.swing.JOptionPane;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

public class AnalyticalQueries extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	URL url=getClass().getResource("tweet_output.txt");
    String pathToFile = url.toString();
 
    public AnalyticalQueries() {
        super();
      
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	
  int option = Integer.parseInt(request.getParameter("option"));
		  
		  switch(option)
			{
			case 1: 
		        AnaltyticalQuery1();		       
		       response.sendRedirect("q1.html");
				break;
			case 2: 
			  AnaltyticalQuery2();	  
			response.sendRedirect("q2.html");
			break;
		case 3:
			 AnaltyticalQuery3();	
			response.sendRedirect("q3.html");
			break;
		case 4:
			AnaltyticalQuery4();
			response.sendRedirect("q4.html");
			break;
		case 5:
			AnaltyticalQuery5();
			response.sendRedirect("q5.html");
			break;
		case 6:
			AnaltyticalQuery6();
			response.sendRedirect("q6.html");
			break;
		case 7:
			AnaltyticalQuery7();
			response.sendRedirect("q7.html");
			break;
		case 8:
			AnaltyticalQuery8();
			response.sendRedirect("q8.html");
			break;
		
			default: //JOptionPane.showMessageDialog(null, "Invalid Option");
						
				AnaltyticalQuery8();
				response.sendRedirect("q8.html");
				break;
			}
		}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		  int option = Integer.parseInt(request.getParameter("option"));
		  
		  switch(option)
			{
			case 1: 
		        AnaltyticalQuery1();		       
		       response.sendRedirect("q1.html");
				break;
			case 2: 
			  AnaltyticalQuery2();	  
			response.sendRedirect("q2.html");
			break;
		case 3:
			 AnaltyticalQuery3();	
			response.sendRedirect("q3.html");
			break;
		case 4:
			AnaltyticalQuery4();
			response.sendRedirect("q4.html");
			break;
		case 5:
			AnaltyticalQuery5();
			response.sendRedirect("q5.html");
			break;
		case 6:
			AnaltyticalQuery6();
			response.sendRedirect("q6.html");
			break;
		case 7:
			AnaltyticalQuery7();
			response.sendRedirect("q7.html");
			break;
		case 8:
			AnaltyticalQuery8();
			response.sendRedirect("q8.html");
			break;
		
			default: JOptionPane.showMessageDialog(null, "Invalid Option");
						break;
			}
				
	}
		
	public void AnaltyticalQuery1()
	{
		 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");
		 
		    JavaSparkContext sc = new JavaSparkContext(conf);
		   
		    JavaSQLContext sqlContext = new JavaSQLContext(sc);
		   
		    JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

		    tweets.registerAsTable("tweetTable");

		    tweets.printSchema();

		    nbTweetByTeam(sqlContext);

		    sc.stop();
	}
	
	
	private void nbTweetByTeam(JavaSQLContext sqlContext) 
	{		  
		  try
		  {
			  File outputFile = new File(getServletContext().getRealPath("/")
			            + "/q1.csv");
			
		 FileWriter fw= new FileWriter(outputFile);

	    JavaSchemaRDD count = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
	    										"WHERE text LIKE '%Kings XI%'");
	    JavaSchemaRDD count1 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
				"WHERE text LIKE '%KKR%'");
	    JavaSchemaRDD count2 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
				"WHERE text LIKE '%RCB%'");
	    JavaSchemaRDD count3 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
				"WHERE text LIKE '%SRH%'");
	    JavaSchemaRDD count4 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
				"WHERE text LIKE '%Mumbai%'");
	    JavaSchemaRDD count5 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
				"WHERE text LIKE '%Delhi%'");
	 
	 List<Row> kings=count.collect();	 
	 String kings12=kings.toString();
	 String kings1 = kings12.substring(kings12.indexOf("[") + 2, kings12.indexOf("]"));
	 
	 List<Row> kkr=count1.collect();
	 String kkr12=kkr.toString();
	 String kkr1 = kkr12.substring(kkr12.indexOf("[") + 2, kkr12.indexOf("]"));
	 
	 List<Row> RCB=count2.collect();
	 String RCB12=RCB.toString();
	 String RCB1 = RCB12.substring(RCB12.indexOf("[") + 2, RCB12.indexOf("]"));
	 
	 List<Row> SRH=count3.collect();
	 String SRH12=SRH.toString();
	 String SRH1 = SRH12.substring(SRH12.indexOf("[") + 2, SRH12.indexOf("]"));
	 
	 List<Row> MI=count4.collect();
	 String MI12=MI.toString();
	 String MI1 = MI12.substring(MI12.indexOf("[") + 2, MI12.indexOf("]"));
	 
	 List<Row> Delhi=count5.collect();
	 String Delhi12=Delhi.toString();
	 String Delhi1 = Delhi12.substring(Delhi12.indexOf("[") + 2, Delhi12.indexOf("]"));
	
	    
	    fw.append("TeamName");
		fw.append(',');
		fw.append("Count");
		fw.append("\n");
		fw.append("Kings XI");
		fw.append(',');
		fw.append(kings1);
		fw.append("\n");
		fw.append("KKR");
		fw.append(',');
		fw.append(kkr1);
		fw.append("\n");
		fw.append("RCB");
		fw.append(',');
		fw.append(RCB1);
		fw.append("\n");
		fw.append("SRH");
		fw.append(',');
		fw.append(SRH1);
		fw.append("\n");
		fw.append("Mumbai Indians");
		fw.append(',');
		fw.append(MI1);
		fw.append("\n");
		fw.append("DDD");
		fw.append(',');
		fw.append(Delhi1);
		fw.append("\n");
		
		
		
		fw.close();
		
		
	 }
		  catch (Exception exp)
		  {
		  }
		  
	  }
	
	public void AnaltyticalQuery2()
	{
		 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

         JavaSparkContext sc = new JavaSparkContext(conf);
 
         JavaSQLContext sqlContext = new JavaSQLContext(sc);

         JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

         tweets.registerAsTable("tweetTable");

        tweets.printSchema();

       nbIPLPopular(sqlContext);

        sc.stop();
	}
	
	
	 private void nbIPLPopular(JavaSQLContext sqlContext) {
		  
		  try
		  {
	    File outputFile = new File(getServletContext().getRealPath("/")
			            + "/q2.csv");
			
		 FileWriter fw= new FileWriter(outputFile);
	   
	    JavaSchemaRDD count = sqlContext.sql("SELECT DISTINCT  user.screen_name, user.followers_count AS c FROM tweetTable " +
	                                         "ORDER BY c");
	    
	    List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 

	       Collections.reverse(rows);
		    
		   String rows123=rows.toString();
	    
	       String[] array = rows123.split("],"); 
	    
	
	    
	    fw.append("Name");
		fw.append(',');
		fw.append("Count");
		fw.append("\n");
		
		

		for(int i = 0; i < 8; i++)
		{
			if(i==0)
			{
				fw.append(array[0].substring(2));
				fw.append(',');
				fw.append("\n");
			}
			else {
			fw.append(array[i].substring(2));
			fw.append(',');
		    fw.append("\n");
			}
		}
		
		fw.close();
		
		
	 }
		  catch (Exception exp)
		  {
		  }
		  

	  }
	
	
	public void AnaltyticalQuery3()
	{
		 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");
		    
		    
		    JavaSparkContext sc = new JavaSparkContext(conf);
		   
		    JavaSQLContext sqlContext = new JavaSQLContext(sc);
		   
		    JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

		    tweets.registerAsTable("tweetTable");

		    tweets.printSchema();

		    nbmosttwittingUser(sqlContext);

		    sc.stop();
		    
	}
	
	private void nbmosttwittingUser(JavaSQLContext sqlContext) 
	{		  
		 try
		 {
			 
			 File outputFile = new File(getServletContext().getRealPath("/")
			            + "/q3.csv");
			
		 FileWriter fw= new FileWriter(outputFile);
	   
	    JavaSchemaRDD count = sqlContext.sql("SELECT DISTINCT user.name,user.statuses_count AS c FROM tweetTable " +
	    		                             "ORDER BY c");
	    
	    
       List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 

       Collections.reverse(rows);
	    
	    String rows123=rows.toString();
	    
	   
	    
	   String[] array = rows123.split("],"); 
	    
	    System.out.println(rows123);
	    
	    fw.append("Name");
		fw.append(',');
		fw.append("Count");
		fw.append("\n");
		
		

		for(int i = 0; i < 8; i++)
		{
			if(i==0)
			{
				fw.append(array[0].substring(2));
				fw.append(',');
				fw.append("\n");
			}
			else {
			fw.append(array[i].substring(2));
			fw.append(',');
			fw.append("\n");
			}
		}
		
		fw.close();
		
		
	 }
		  catch (Exception exp)
		  {
		  }

	  }

	public void AnaltyticalQuery4()
	{
		 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

	       JavaSparkContext sc = new JavaSparkContext(conf);

	       JavaSQLContext sqlContext = new JavaSQLContext(sc);

	       JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

	       tweets.registerAsTable("tweetTable");

	       tweets.printSchema();

	       nbTweetpositiveNegative(sqlContext);

	       sc.stop();
	}
	
	private void nbTweetpositiveNegative(JavaSQLContext sqlContext) 
	
	{
	    try
		{
	     
	    	 File outputFile = new File(getServletContext().getRealPath("/")
			            + "/q4.csv");
			
		 FileWriter fw= new FileWriter(outputFile);
	   
	  JavaSchemaRDD count = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
	  										"WHERE text LIKE '%abundant%' OR text LIKE '%accessible%' OR text LIKE '%accurate%' OR text LIKE '%award%' OR text LIKE '%awesome%' OR text LIKE '%beautiful%' OR text LIKE '%affirmation%' OR text LIKE '%amicable%' OR text LIKE '%appreciate%' OR text LIKE '%approve%' OR text LIKE '%attractive%' OR text LIKE '%benefit%' OR text LIKE '%bless%' OR text LIKE '%bonus%' OR text LIKE '%brave%' OR text LIKE '%bright%' OR text LIKE '%brilliant%' OR text LIKE '%celebrate%' OR text LIKE '%champion%' OR text LIKE '%charm%' OR text LIKE '%cheer%' OR text LIKE '%clever%' OR text LIKE '%colorful%' OR text LIKE '%comfort%' OR text LIKE '%compliment%' OR text LIKE '%confidence%' OR text LIKE '%congratulation%' OR text LIKE '%cute%' OR text LIKE '%good%' OR text LIKE '%happy%' OR text LIKE '%cool%' OR text LIKE '%easy%' OR text LIKE '%effective%' OR text LIKE '%efficient%' OR text LIKE '%fair%' OR text LIKE '%excite%' OR text LIKE '%fast%' OR text LIKE '%fine%' OR text LIKE '%fortunate%' OR text LIKE '%free%' OR text LIKE '%fresh%' OR text LIKE '%fun%' OR text LIKE '%gain%' OR text LIKE '%gem%' OR text LIKE '%gorgeous%' OR text LIKE '%grand%' OR text LIKE '%handsome%' OR text LIKE '%healthy%' OR text LIKE '%honest%' OR text LIKE '%humor%' OR text LIKE '%important%' OR text LIKE '%impress%' OR text LIKE '%improve%' OR text LIKE '%joy%' OR text LIKE '%love%' OR text LIKE '%perfect%' OR text LIKE '%pleasant%' OR text LIKE '%compliment%' OR text LIKE '%pleasure%' OR text LIKE '%precious%' OR text LIKE '%prolific%' OR text LIKE '%prudent%' OR text LIKE '%happy%' OR text LIKE '%cool%' OR text LIKE '%proven%' OR text LIKE '%effective%' OR text LIKE '%efficient%' OR text LIKE '%restored%'  ");
	  JavaSchemaRDD count1 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
				"WHERE text LIKE '%abuse%' OR text LIKE '%abyss%' OR text LIKE '%absurd%' OR text LIKE '%akward%' OR text LIKE '%adverse%' OR text LIKE '%agony%' OR text LIKE '%annoying%' OR text LIKE '%anti%' OR text LIKE '%arrogant%' OR text LIKE '%assassinate%' OR text LIKE '%aversion%' OR text LIKE '%backward%' OR text LIKE '%bad%' OR text LIKE '%brutal%' OR text LIKE '%battered%' OR text LIKE '%berate%' OR text LIKE '%bewitch%' OR text LIKE '%berate%' OR text LIKE '%blunder%' OR text LIKE '%complain%' OR text LIKE '%conflict%' OR text LIKE '%confound%' OR text LIKE '%contagious%' OR text LIKE '%contaminated%' OR text LIKE '%contravene%' OR text LIKE '%corruption%' OR text LIKE '%corrupt%' OR text LIKE '%coward%' OR text LIKE '%cruel%' OR text LIKE '%sad%' OR text LIKE '%danger%' OR text LIKE '%debase%' OR text LIKE '%decline%' OR text LIKE '%deceive%' OR text LIKE '%defamation%' OR text LIKE '%demon%' OR text LIKE '%demolish%' OR text LIKE '%denied%' OR text LIKE '%demolish%' OR text LIKE '%depress%' OR text LIKE '%deny%' OR text LIKE '%destroy%' OR text LIKE '%devastation%' OR text LIKE '%disadvantage%' OR text LIKE '%disappointed%' OR text LIKE '%discord%' OR text LIKE '%evil%' OR text LIKE '%gossip%' OR text LIKE '%hard%' OR text LIKE '%gloom%' OR text LIKE '%hate%' OR text LIKE '%hazard%' OR text LIKE '%fuck%' OR text LIKE '%horrible%' OR text LIKE '%idiot%' OR text LIKE '%imperfect%' OR text LIKE '%inefficient%' OR text LIKE '%inflammation%' OR text LIKE '%ironic%' OR text LIKE '%irritate%' OR text LIKE '%jealous%' OR text LIKE '%lag%' OR text LIKE '%lie%' OR text LIKE '%malignant%' OR text LIKE '%malign%' OR text LIKE '%noisy%' OR text LIKE '%odd%' OR text LIKE '%offence%' OR text LIKE '%offend%' OR text LIKE '%offensive%' OR text LIKE '%bad%' OR text LIKE '%unhappy%' OR text LIKE '%weak%'");
	 
	 List<Row> positive=count.collect();	 
	 String positive12=positive.toString();
	 String positive1 = positive12.substring(positive12.indexOf("[") + 2, positive12.indexOf("]"));
	 
	 List<Row> negative=count1.collect();
	 String negative12=negative.toString();
	 String negative1 = negative12.substring(negative12.indexOf("[") + 2, negative12.indexOf("]"));
	 
	    
	    fw.append("Words");
		fw.append(',');
		fw.append("Count");
		fw.append("\n");
		fw.append("PositiveTweets");
		fw.append(',');
		fw.append(positive1);
		fw.append("\n");
		fw.append("NegativeTweets");
		fw.append(',');
		fw.append("-"+negative1);
		fw.append("\n");
		
		
		
		
		fw.close();
		
		
	 }
		  catch (Exception exp)
		  {
		  }
		  

	  }
	public void AnaltyticalQuery5()
	{
		 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

         JavaSparkContext sc = new JavaSparkContext(conf);
         JavaSQLContext sqlContext = new JavaSQLContext(sc);

         JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

        tweets.registerAsTable("tweetTable");

       tweets.printSchema();

          nbTweetTimes(sqlContext);

       sc.stop();
	}
	
	private void nbTweetTimes(JavaSQLContext sqlContext) 
	 {		  
		  try
		  {
			  File outputFile = new File(getServletContext().getRealPath("/")
			            + "/q5.csv");
			
		 FileWriter fw= new FileWriter(outputFile);
	   
	    JavaSchemaRDD count = sqlContext.sql("SELECT created_at, COUNT(*) AS c FROM tweetTable " +
	    										"Group By created_at " +
	    		                                  "order by c" );
	    	   
	    List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 
	    
       Collections.reverse(rows);
	    
	    String rows123=rows.toString();
	    
	    String[] array = rows123.split("],"); 
	    
	    System.out.println(rows123);
	    
	    fw.append("Time");
		fw.append(',');
		fw.append("Count");
		fw.append("\n");
		
		

		for(int i = 0; i < 9; i++)
		{
			if(i==0)
			{
				continue;
			}
			else if(i == array.length-1)
			{
				fw.append(array[i].substring(2,array[i].length()-2));
				fw.append(',');
				fw.append("\n");
			}
			else {
			fw.append(array[i].substring(2));
			fw.append(',');
			fw.append("\n");
			}
		}
		
		fw.close();
		
		
	 }
		  catch (Exception exp)
		  {
		  }
		  

	  }
	public void AnaltyticalQuery6()
	{
		 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

         JavaSparkContext sc = new JavaSparkContext(conf);

         JavaSQLContext sqlContext = new JavaSQLContext(sc);

         JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

         tweets.registerAsTable("tweetTable");

         tweets.printSchema();

          nbMoreFriends(sqlContext);

          sc.stop();
	}
	
	 private void nbMoreFriends(JavaSQLContext sqlContext) {
		  
		  try
		  {
			  File outputFile = new File(getServletContext().getRealPath("/")
			            + "/q6.csv");
			
		 FileWriter fw= new FileWriter(outputFile);
	   
	    JavaSchemaRDD count = sqlContext.sql("SELECT DISTINCT user.screen_name, user.friends_count AS c FROM tweetTable " +
	    										"WHERE text like '%RCB%'" +
	    		                                  "order by c" ); 
	   
	    List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 
	    
      Collections.reverse(rows);
	    
	    String rows123=rows.toString();
	    
	   String[] array = rows123.split("],"); 
	    
	    System.out.println(rows123);
	    
	    fw.append("Name");
		fw.append(',');
		fw.append("Count");
		fw.append("\n");
		
		

		for(int i = 0; i < 9; i++)
		{
			if(i==0)
			{
				fw.append(array[0].substring(2));
				fw.append(',');
				fw.append("\n");
			}
			else if(i == array.length-1)
			{
				fw.append(array[i].substring(2,array[i].length()-2));
				fw.append(',');
				fw.append("\n");
			}
			else {
			fw.append(array[i].substring(2));
			fw.append(',');
			fw.append("\n");
			}
		}
		
		fw.close();
		
		
	 }
		  catch (Exception exp)
		  {
		  }
		  

	  }
	public void AnaltyticalQuery7()
	{
		 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

         JavaSparkContext sc = new JavaSparkContext(conf);

         JavaSQLContext sqlContext = new JavaSQLContext(sc);

         JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

         tweets.registerAsTable("tweetTable");

         tweets.printSchema();

          nbLanguage(sqlContext);

          sc.stop();
	}
	
	 private void nbLanguage(JavaSQLContext sqlContext) {
		  
		  try
		  {
			  File outputFile = new File(getServletContext().getRealPath("/")
			            + "/q7.csv");
			
		 FileWriter fw= new FileWriter(outputFile);
	   
	    JavaSchemaRDD count = sqlContext.sql("SELECT lang, COUNT(*) AS c FROM tweetTable " +
	    										"Group By lang " +
	    		                                  "order by c"); 
	   
	    List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 
	    
     Collections.reverse(rows);
	    
	    String rows123=rows.toString();
	    
	   String[] array = rows123.split("],"); 
	    
	    System.out.println(rows123);
	    
	    fw.append("Language");
		fw.append(',');
		fw.append("Count");
		fw.append("\n");
		
		

		for(int i = 0; i < 9; i++)
		{
			if(i==0)
			{
				continue;
			}
			else if(i == array.length-1)
			{
				fw.append(array[i].substring(2,array[i].length()-2));
				fw.append(',');
				fw.append("\n");
			}
			else {
			fw.append(array[i].substring(2));
			fw.append(',');
			fw.append("\n");
			}
		}
		
		fw.close();
		
		
	 }
		  catch (Exception exp)
		  {
		  }
		  

	  }
	
	public void AnaltyticalQuery8()
	{
		 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");
		   
		    JavaSparkContext sc = new JavaSparkContext(conf);

		    JavaSQLContext sqlContext = new JavaSQLContext(sc);

		    JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

		    tweets.registerAsTable("tweetTable");

		    tweets.printSchema();

		    nbCaptain(sqlContext);

		    sc.stop();
	}
	 private void nbCaptain(JavaSQLContext sqlContext) 
	 {
	 		  
		  try
		  {
			  File outputFile = new File(getServletContext().getRealPath("/")
			            + "/q8.csv");
			
		 FileWriter fw= new FileWriter(outputFile);
	   
	     JavaSchemaRDD totalcount = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable ");
	    
	     JavaSchemaRDD count1 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
	  										  "WHERE text like '%dhoni%' ");
	    
	     JavaSchemaRDD count2 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
				                               "WHERE text like '%rohit%' ");
	     
	     JavaSchemaRDD count3 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
                 "WHERE text like '%gambhir%' ");
	     
	     JavaSchemaRDD count4 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
                 "WHERE text like '%kohli%' ");
	     
	     JavaSchemaRDD count5 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
                 "WHERE text like '%raina%' ");
	 
	 
	 List<Row> rows=totalcount.collect();	 
	 String rows12=rows.toString();
	 String rows1 =rows12.substring(rows12.indexOf("[") + 2, rows12.indexOf("]"));
	 
	 List<Row> dhonicount=count1.collect();	 
	 String dhonicount12=dhonicount.toString();
	 String dhonicount1 = dhonicount12.substring(dhonicount12.indexOf("[") + 2, dhonicount12.indexOf("]"));
	 
	 List<Row> rohitcount=count2.collect();	 
	 String rohitcount12=rohitcount.toString();
	 String rohitcount1 = rohitcount12.substring(rohitcount12.indexOf("[") + 2, rohitcount12.indexOf("]"));
	 
	 List<Row> gambhircount=count3.collect();	 
	 String gambhircount12=gambhircount.toString();
	 String gambhircount1 = gambhircount12.substring(gambhircount12.indexOf("[") + 2, gambhircount12.indexOf("]"));
	 
	 List<Row> rainacount=count5.collect();	 
	 String rainacount12=rainacount.toString();
	 String rainacount1 = rainacount12.substring(rainacount12.indexOf("[") + 2, rainacount12.indexOf("]"));
	
	 List<Row> kohlicount=count4.collect();	 
	 String kohlicount12=kohlicount.toString();
	 String kohlicount1 = kohlicount12.substring(kohlicount12.indexOf("[") + 2, kohlicount12.indexOf("]"));
	 
	int rows123=Integer.parseInt(rows1);
	
	int dhonitweet123=Integer.parseInt(dhonicount1);
	
	int rohittweet123=Integer.parseInt(rohitcount1);
	
    int gambhirtweet123=Integer.parseInt(gambhircount1);
	
	int kohlitweet123=Integer.parseInt(kohlicount1);
	
	int rainatweet123=Integer.parseInt(rainacount1);
	
	System.out.println(rows123);
	System.out.println(dhonitweet123);
	System.out.println(rohittweet123);
	System.out.println(gambhirtweet123);
	System.out.println(kohlitweet123);
	
	double dhoniPercentage=((dhonitweet123*1000)/rows123);
	double rohitPercentage=((rohittweet123*10000)/rows123);
	double gambhirPercentage=((gambhirtweet123*10000)/rows123);
	double kohliPercentage=((kohlitweet123*1000)/rows123);
	double rainaPercentage=((rainatweet123*10000)/rows123);
	

	 
	String dhoniPercentage1=Double.toString(dhoniPercentage);
	String rohitPercentage1=Double.toString(rohitPercentage);
	String gambhirPercentage1=Double.toString(gambhirPercentage);
	String kohliPercentage1=Double.toString(kohliPercentage);
	String rainaPercentage1=Double.toString(rainaPercentage);
	
	
	    
	    fw.append("TweetStatus");
		fw.append(',');
		fw.append("Percentage");
		fw.append("\n");
		fw.append("Dhoni");
		fw.append(',');
		fw.append(dhoniPercentage1);
		fw.append("\n");
		fw.append("Rohit");
		fw.append(',');
		fw.append(rohitPercentage1);
		fw.append("\n");
		fw.append("gambhir");
		fw.append(',');
		fw.append(gambhirPercentage1);
		fw.append("\n");
		fw.append("Kohli");
		fw.append(',');
		fw.append(kohliPercentage1);
		fw.append("\n");
		fw.append("Raina");
		fw.append(',');
		fw.append(rainaPercentage1);
		fw.append("\n");
		
		
			
		
		fw.close();
		
		
	 }
		  catch (Exception exp)
		  {
		  }
		  

	  }

}
