package hadoopts.loader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import org.json.simple.*;


import hadoopts.core.TimeSeries;
import hadoopts.viewer.MultiTimeSeriesChart;

/**
 * @author Mirko Kaempf
 */
public class StockDataLoader {

    private static final Logger LOG = Logger.getLogger(StockDataLoader.class.getName());

    private static void loadCacheYears(int[] years) {
        
        String column = "Close";
        String label = "UK.csv";

        for( int year : years ) {
            
            try {
                
                StockDataLoader sDL_1 = StockDataLoader.getOnlineLoader(column, year + "-01-01", year + "-12-31", label, year);
                sDL_1.showCharts();
                
            } 
            catch (IOException ex) {
                ex.printStackTrace();
            }

        }
    }

    boolean debug = false;

    /**
     * The local cache functionality is considered to be useful for the
     * local workstation mode.
     * 
     * For cluster processing we create RDDs from the locally cached data. 
     * 
     * @throws FileNotFoundException
     * @throws IOException 
     */
    private void initLocalCache() throws FileNotFoundException, IOException {
        loadListe();
        loadDataFromFile();
    }

    Vector<String> liste = new Vector<String>(); // name list ...
    Hashtable<String, String> hash = new Hashtable<String, String>();
    Hashtable<String, TimeSeries> hashMR = new Hashtable<String, TimeSeries>();

    // name of the label listfile for which the stock-prices are loaded
    String label = "UK.csv";

    // query parameters 
    String stock = "MSFT";
    String startDate = "2009-01-01";
    String endDate = "2009-12-31";
    String column = "Adj_Close";

    /**
     * Loads locally cached data
     * @param from
     * @param to
     * @param label
     * @return
     * @throws FileNotFoundException
     * @throws IOException 
     */
    public static StockDataLoader getLocalLoader(String from, String to, String lab) throws FileNotFoundException, IOException {
        StockDataLoader l = new StockDataLoader();
        l.startDate = from;
        l.endDate = to;
        l.label = lab;
        l.initLocalCache();
        return l;
    }

    /**
     * Loads data from Yahoo Financial Services ...
     * 
     * @param column
     * @param from
     * @param to
     * @param label
     * @return
     * @throws FileNotFoundException
     * @throws IOException 
     */
    public static StockDataLoader getOnlineLoader(String column, String from, String to, String lab, int year) throws FileNotFoundException, IOException {
        StockDataLoader l = new StockDataLoader();
        l.startDate = from;
        l.endDate = to;
        l.year = year;
        l.label = lab;
        l.initCacheFromWeb(column);
        return l;
    }

    /**
     * For each year in the years array we load the time series from
     * Yahoo Financial Services.
     * 
     * @param arg
     * @throws IOException 
     */
    public static void main(String[] arg) throws IOException {

//        int[] years = {1995,1996,1997,1998};
//        int[] years = {1999,2000,2001,2002};
//        int[] years = {2003,2004,2005,2006};
//        int[] years = {2007,2008,2009,2010};
//        int[] years = {2011,2012,2013,2014};
        int[] years = {1999, 2003, 2010};

        loadCacheYears( years );
        
        /**
         * Example 1: Load the Close price for 2012 from web and cache locally
         */
        String column = "Close";
        
        String label = "UK.csv";

//        
//        StockDataLoader sDL_1 = StockDataLoader.getOnlineLoader(column, "2012-01-01", "2012-12-31", label, 2012);
//        
//        sDL_1.initColumn("Adj_Close");
//        sDL_1.showCharts();
//        
//        sDL_1.initColumn("High");
//        sDL_1.showCharts();
//        
//        int i=0;
//        for (String s : sDL_1.bad) {
//            i++;
//            System.out.println("> Could not load data for: " + s);
//        }
//        System.out.println("> Total Errors: " + i);

//        /**
//         * Now we extract other columns from local files ...
//         */
//        StockDataLoader sdl = StockDataLoader.getLocalLoader("2012-01-01", "2012-12-31", label);
//        sdl.initColumn("Close");
//        sdl.showCharts();
//
//        StockDataLoader sdl2 = StockDataLoader.getLocalLoader("2012-01-01", "2012-12-31", label);
//        sdl2.initColumn("Adj_Close");
//        sdl2.showCharts();
//
//        StockDataLoader sdl3 = StockDataLoader.getLocalLoader("2012-01-01", "2012-12-31", label);
//        sdl3.initColumn("High");
//        sdl3.showCharts();
//
//        StockDataLoader sdl4 = StockDataLoader.getLocalLoader("2012-01-01", "2012-12-31", label);
//        sdl4.initColumn("Low");
//        sdl4.showCharts();
//
//        StockDataLoader sdl5 = StockDataLoader.getLocalLoader("2012-01-01", "2012-12-31", label);
//        sdl5.initColumn("Open");
//        sdl5.showCharts();
//
//        StockDataLoader sdl6 = StockDataLoader.getLocalLoader("2012-01-01", "2012-12-31", label);
//        sdl6.initColumn("Volume");
//        sdl6.showCharts();
    }

    // local time series container which becomes an RDD later on.
    Vector<TimeSeries> vmr = new Vector<TimeSeries>();
    int year = 2001;
    
    public void loadForSymbol(String symbol, String selectedColumn, BufferedWriter bw) throws IOException {

//        String callUrl = "http://query.yahooapis.com/v1/public/yql?q=select * from yahoo.finance.historicaldata where symbol in (" + key + "\") and startDate=\"" + startDate + "\" and endDate=\"" + endDate + "\"&diagnostics=true&env=http://datatables.org/alltables.env&format=json";
//        String callUrl = "http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.historicaldata%20where%20symbol%20in%20%28%22" + symbol + "%22%29%20and%20startDate=%222009-01-01%22%20and%20endDate=%222009-12-31%22&diagnostics=true&env=http://datatables.org/alltables.env&format=json";
        String callUrl = "http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.historicaldata%20where%20symbol%20in%20%28%22" + symbol + "%22%29%20and%20startDate=%22"+year+"-01-01%22%20and%20endDate=%22"+year+"-12-31%22&diagnostics=true&env=http://datatables.org/alltables.env&format=json";

        System.out.println(callUrl);

        bw.write(symbol + "\t");
        bw.write(callUrl + "\t");

        DefaultHttpClient httpClient = new DefaultHttpClient();
        HttpGet httpget = new HttpGet(callUrl);

        HttpResponse response = httpClient.execute(httpget);

        if ( debug ) {
            System.out.println(response.getProtocolVersion());
            System.out.println(response.getStatusLine().getStatusCode());
            System.out.println(response.getStatusLine().getReasonPhrase());
            System.out.println(response.getStatusLine().toString());
        }
        
        BufferedReader br = new BufferedReader(
                new InputStreamReader((response.getEntity().getContent())));

        StringBuffer buffer = new StringBuffer();

        String output;
        
        if ( debug ) System.out.println("Output from Server .... \n");
        
        while ((output = br.readLine()) != null) {
            System.out.println(output);
            buffer = buffer.append(output);
        }
        
        
        try {
            String s = buffer.toString();
            temp.put(symbol, s);

            bw.write(s + "\n");

            Object obj = JSONValue.parse(s);
            JSONObject jsonObject = (JSONObject) obj;
            JSONObject query = (JSONObject) jsonObject.get("query");
            JSONObject res = (JSONObject) query.get("results");
            JSONArray qt = (JSONArray) res.get("quote");

//            System.out.println("*** [" + qt + "]");

            String s2 = res.toJSONString();
            int i = 0;

            // This is the time series we are working with
            TimeSeries mr = new TimeSeries();
            mr.setLabel(symbol + "_" + selectedColumn);

            /** TODO: Add better metadata handling **/
                        
            Hashtable<Long, Double> data = new Hashtable<Long, Double>();

            while (i < qt.size()) {
                JSONObject val = (JSONObject) qt.get(i);
                String b = (String) val.get(selectedColumn);
        
                // this fails sometimes ... it worked with "date" and "Date" ...
                String a = (String) val.get("Date");

                DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

                Date date = df.parse(a);

                data.put(date.getTime(), Double.parseDouble(b));

//                System.out.println(date.getTime() + " # " + a + " : " + b);
//                System.out.println("{" + qt.get(i) + "}");
                i++;
            }

            Set<Long> k = data.keySet();
            ArrayList<Long> liste = new ArrayList<Long>();
            liste.addAll(k);

            Collections.sort(liste);

            for (Long key2 : k) {
//                System.out.println(key2 + " : " + data.get(key2));
                mr.addValuePair(key2, data.get(key2));
            }

            vmr.add(mr);

        } catch (Exception pe) {
            Logger.getLogger(StockDataLoader.class.getName()).log(Level.SEVERE, null, pe);

            System.out.println(pe.getMessage());

            bad.add(symbol);
        }
    }

    // lets track the symbols for which no data was collected.
    Vector<String> bad = new Vector<String>();

    
    boolean isFilled = false;
    /**
     * The list of Symbols is loaded locally.
     * 
     * @throws FileNotFoundException
     * @throws IOException 
     */
    private void loadListe() throws FileNotFoundException, IOException {
        if ( isFilled ) return;
        String file = "./.example-data/STOCK/" + label;
        File f = new File(file);
        if ( !f.exists() ) f.mkdirs();
        
        FileReader fr = new FileReader( f );
        BufferedReader br = new BufferedReader(fr);
        while (br.ready()) {
            String line = br.readLine();

            if (line.startsWith("#")) {
            } else {

                StringTokenizer st = new StringTokenizer(line, "\t");
                System.out.println(st.countTokens() + ":" + line);
                String tok1 = st.nextToken();
                String tok2 = st.nextToken();
                System.out.println("(" + tok1 + ") [" + tok2 + "]");

                hash.put(tok1, tok2);

                if (!liste.contains(tok2)) {
                    liste.add(tok2);
                }
            }
        }
//        hash.put("Visa_Inc.","V");
//        hash.put("Walmart","WMT");
//        hash.put("The_Walt_Disney_Company","DIS");

        System.out.println("> Stock symbol list loaded : " + f.getAbsolutePath() );
     
    }

    
    Hashtable<String, String> temp = null;

    private void getDataCacheWeb(String column) throws IOException {
        temp = new Hashtable<String, String>();

        int i = 1;

        FileWriter fw = new FileWriter(getFilename());

        BufferedWriter bw = new BufferedWriter(fw);

        for (String s : hash.values()) {
            System.out.println("(" + i + ") SYMBOL: " + s);
            loadForSymbol(s, column, bw);
            i++;
        }
        bw.flush();
        bw.close();
    }
        
    private void loadDataFromWeb(String column) throws IOException {
        temp = new Hashtable<String, String>();

        int i = 1;

        FileWriter fw = new FileWriter(getFilename());

        BufferedWriter bw = new BufferedWriter(fw);

        for (String s : hash.values()) {
            System.out.println("(" + i + ") SYMBOL: " + s);
            loadForSymbol(s, column, bw);
            i++;
        }
        bw.flush();
        bw.close();
    }

    private void initCacheFromWeb(String col) throws FileNotFoundException, IOException {
        column = col;
        loadListe();
        loadDataFromWeb(column);
    }

    private void loadDataFromFile() throws FileNotFoundException, IOException {

        if ( isFilled ) return;

        temp = new Hashtable<String, String>();
        vmr = new Vector<TimeSeries>();
        
        BufferedReader br = new BufferedReader(new FileReader(getFilename()));
        while (br.ready()) {
            String line = br.readLine();
            StringTokenizer st = new StringTokenizer(line, "\t");
            int s = st.countTokens();
            String k = st.nextToken();
            String req = st.nextToken();
            String resp = st.nextToken();
            System.out.println(s + " :" + k + "\n\t" + req + "\n\t\t" + resp);
            temp.put(k, resp);
        }
        System.out.println("> responses reloaded from local cache :" + getFilename() );
        isFilled = true;
    }

    private void showCharts() {
//        System.out.println( vmr.size() + " " + column );
//        System.out.println( vmr.elementAt(0).toString() );
        File folder = new File( getFilename() ).getParentFile();
        String file = label + "_" + startDate + "_" + endDate + "_" + column;
        String comment = "";
        
        MultiTimeSeriesChart.open(vmr, label + "[" + startDate + " ... " + endDate + "]", "t", column, true);
        MultiTimeSeriesChart.openAndStore(vmr, label + "[" + startDate + " ... " + endDate + "]", "t", column, true, folder.getAbsolutePath(), file, comment);
    }

    public String getFilename() {
        File f = new File( "_sdl-cache" );
        if ( !f.exists() ) f.mkdir();
        return "_sdl-cache/RDD-stockdata-" + this.startDate + "_" + this.endDate + "_" + label;
    }

    private void initColumn(String col) {
        bad = new Vector();
        vmr = new Vector();
        
        column = col;
        for (String key : temp.keySet()) {
            String s = temp.get(key);

            try {
            Object obj = JSONValue.parse(s);
            JSONObject jsonObject = (JSONObject) obj;
            JSONObject query = (JSONObject) jsonObject.get("query");
         
            JSONObject res = (JSONObject) query.get("results");
            JSONArray qt = (JSONArray) res.get("quote");

 
            String s2 = res.toJSONString();
            int i = 0;

            TimeSeries mr = new TimeSeries();
            mr.setLabel(key + "_" + column);

            Hashtable<Long, Double> data = new Hashtable<Long, Double>();

            while (i < qt.size()) {
                try {
                    JSONObject val = (JSONObject) qt.get(i);
                    String b = (String) val.get(column);
                    String a = (String) val.get("Date");

                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

                    Date date;

                    date = df.parse(a);

                    data.put(date.getTime(), Double.parseDouble(b));

                    if (debug) {

                       String log1 = date.getTime() + " # " + a + " : " + b;
                       String log2 = "{"+qt.get(i)+"}";
                       
                    }
                } 
                catch (Exception ex) {
                    ex.printStackTrace();
                }
                i++;
            }

            Set<Long> k = data.keySet();
            ArrayList<Long> liste = new ArrayList<Long>();
            liste.addAll(k);

            Collections.sort(liste);

            for (Long key2 : k) {
                //System.out.println(key2 + " : " + data.get(key2));
                mr.addValuePair(key2, data.get(key2));
            }
            vmr.add(mr);
            
            }
            catch(Exception ex){
                System.out.println( key + "   ==> " + ex.getMessage()  );
                bad.add(key);
            }
        }
    }
}
