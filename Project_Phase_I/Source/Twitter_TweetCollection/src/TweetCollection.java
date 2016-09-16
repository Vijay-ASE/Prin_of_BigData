import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import twitter4j.FilterQuery;
 
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

public class TweetCollection
{
	public static FileOutputStream fstream;
	public static int count=0;
 	public static void main(String[] args) throws IOException
	{
        	ConfigurationBuilder config = new ConfigurationBuilder();
        	config.setDebugEnabled(true);
        	config.setJSONStoreEnabled(true);
        	config.setOAuthConsumerKey("FZpwL1DcSJ30iXLS40Bw7fTsL");
        	config.setOAuthConsumerSecret("982epJszcb7s82v35RwdXRvSd845iE9lQfVjV8gROk2TybmPi3");
        	config.setOAuthAccessToken("140825479-vamFkknWGE5xgyu0wPiulP9FBph4yxJhJk7Il9uf");
        	config.setOAuthAccessTokenSecret("foNoaT0tlER2qHUM5pzDXyvbIODJjpzt2yo8KEH94n6lF");
        
        	TwitterStream twitterStream = new TwitterStreamFactory(config.build()).getInstance();
        	File file=new File("d://tweet_output.json");
        
        	try
        	{
        		if (file.exists())
				fstream=new FileOutputStream(file,true);
			else
				fstream=new FileOutputStream(file);
	                	 
         	}	
          	catch (Exception e)
         	{
            		e.printStackTrace();
         	} 
         
        	StatusListener listener = new StatusListener() 
        	{
        	

            		@Override
            		public void onException(Exception arg0) {
            		// TODO Auto-generated method stub

            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice arg0) {
                // TODO Auto-generated method stub

            }

            @Override
            public void onScrubGeo(long arg0, long arg1) {
                // TODO Auto-generated method stub

            }

	    @Override
            public void onStatus(Status status) {
                User user = status.getUser();
             //   status.getRetweetedStatus().
                
                // gets Username
                String username = status.getUser().getScreenName();
                System.out.println(username);
                String profileLocation = user.getLocation();
                System.out.println(profileLocation);
                long tweetId = status.getId(); 
                System.out.println(tweetId);
                String content = status.getText();
                System.out.println(content +"\n");
                
                try {
					fstream.write(TwitterObjectFactory.getRawJSON(status).getBytes());
					fstream.write((byte)'\n');
					fstream.write((byte)'\n');
					fstream.write((byte)'\n');
					System.out.println(count++);
		    } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
              
            }
				@Override
				public void onStallWarning(StallWarning arg0) {
					// TODO Auto-generated method stub
					
				}

				@Override
				public void onTrackLimitationNotice(int arg0) {
					// TODO Auto-generated method stub
					
				}
        
    };
        FilterQuery fq = new FilterQuery();
    
        String keywords[] = {"Obama"};

        fq.track(keywords);

       twitterStream.addListener(listener);
        twitterStream.filter(fq);  

    }
}