/**
 * This class shows multiple time series, optionally with a legend. 
 * All time series have to be from th
 * 
 *
 **/
package hadoopts.viewer;

import hadoopts.core.TimeSeries;
import hadoopts.core.TimeSeriesTable;
import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Container;
import java.awt.Rectangle;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Enumeration;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFrame;
import javax.swing.JTextArea;
import org.apache.batik.dom.GenericDOMImplementation;
import org.apache.batik.svggen.SVGGraphics2D;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartRenderingInfo;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.entity.StandardEntityCollection;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.RefineryUtilities;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;

public class MultiTimeSeriesChart extends javax.swing.JDialog {

    public static Color bgCOLOR = Color.white;

    public static void open2(Vector<TimeSeries> rows, boolean b) {
       
        open( rows, b );
    }

    public static void _initColors(Color[] colors) {
        dcSet = colors;
    }

    
    
    public static void _setTypes(int[] _dt) {
         useMyStroke = true;
         dt = _dt;
    }
                
    public String chartTitle = "untitled";
    public String xLabel = "x";
    public String yLabel = "y";
    public boolean useLegende = false;

    public boolean doStoreChart = true;
    public String filename = null;


    public JFreeChart chart = null;
    public XYSeriesCollection dataset = new XYSeriesCollection();


    public MultiTimeSeriesChart() {};
    
    public MultiTimeSeriesChart(String label) {
        super(new JFrame(label), false);
        initComponents();
        this.setSize(800, 600);
        initChart();
    }
    

    /** Creates new form MultiChart */
    public MultiTimeSeriesChart(java.awt.Frame parent, boolean modal) {
        super(parent, modal);
        initComponents();
        this.setSize(800, 600);
        initChart();
    }
    
    public Object getCP() { 
        return cp;
    }
    
    public static double xRangDEFAULT_MIN = 0.0;
    public static double xRangDEFAULT_MAX = 170.0;
    public static double yRangDEFAULT_MAX = 6000.0;
    public static double yRangDEFAULT_MIN = 0.0;

 

    /** This method is called from within the constructor to
     * initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is
     * always regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    public void initComponents() {

        jPanel1 = new javax.swing.JPanel();
        chartPanel = new javax.swing.JPanel();
        jPanel2 = new javax.swing.JPanel();
        jScrollPane1 = new javax.swing.JScrollPane();
        statisticTextField = new javax.swing.JTextArea();
        jPanel3 = new javax.swing.JPanel();
        jButton1 = new javax.swing.JButton();

        setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);

        jPanel1.setLayout(new java.awt.BorderLayout());

        chartPanel.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));
        chartPanel.setLayout(new java.awt.BorderLayout());
        jPanel1.add(chartPanel, java.awt.BorderLayout.CENTER);

        jPanel2.setLayout(new java.awt.BorderLayout());

        jScrollPane1.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));

        statisticTextField.setColumns(20);
        statisticTextField.setRows(5);
        jScrollPane1.setViewportView(statisticTextField);

        jPanel2.add(jScrollPane1, java.awt.BorderLayout.CENTER);

        jPanel1.add(jPanel2, java.awt.BorderLayout.EAST);

        jPanel3.setPreferredSize(new java.awt.Dimension(400, 47));

        jButton1.setText("Close");
        jButton1.addActionListener(new java.awt.event.ActionListener() {

            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton1ActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
        jPanel3.setLayout(jPanel3Layout);
        jPanel3Layout.setHorizontalGroup(
                jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel3Layout.createSequentialGroup().addContainerGap(331, Short.MAX_VALUE).addComponent(jButton1).addContainerGap()));
        jPanel3Layout.setVerticalGroup(
                jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel3Layout.createSequentialGroup().addContainerGap(13, Short.MAX_VALUE).addComponent(jButton1).addContainerGap()));

        jPanel1.add(jPanel3, java.awt.BorderLayout.PAGE_END);

        getContentPane().add(jPanel1, java.awt.BorderLayout.CENTER);

        pack();
    }

    private void jButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed
        this.setVisible(false);
    }

//    /**
//     * @param args the command line arguments
//     */
//    public static void save(final Messreihe mr) {
//        java.awt.EventQueue.invokeLater(new Runnable() {
//
//            public void run() {
//                final MultiChart dialog = new MultiChart(new javax.swing.JFrame(), false);
//                dialog.addWindowListener(new java.awt.event.WindowAdapter() {
//
//                    public void windowClosing(java.awt.event.WindowEvent e) {
//                        dialog.setVisible(false);
//                    }
//                });
//                dialog.initMessreihe(mr);
//            }
//        });
//    }

    public static void open(TimeSeries[] mrs, final String title, final String x, final String y, final boolean b) {
        Vector<TimeSeries> mr = new Vector<TimeSeries>();
        for( int i = 0 ; i < mrs.length; i++ ) {
            mr.add( mrs[i] );
        }
        open(  mr, title ,x ,y ,b, "no comment" );
    }

    public static void open(TimeSeries[] mrs ) {
        open(  mrs, "?" ,"x","y", true );
    }
    public static void open(Vector<TimeSeries> mrs ) {
         open(  mrs, "?" ,"x","y", true , "");
    }
    public static void open(Vector<TimeSeries> mrs, boolean legende ) {
         open(  mrs, "?" ,"x","y", legende ,"");
    }
    public static void open(Vector<TimeSeries> mrs, boolean legende, String label) {
         open(  mrs, label ,"RR","m", legende , "" );
    }

    public static void open(final Vector<TimeSeries> mrs, final String string, final String x, final String y, final boolean b , final String comment) {
//        java.awt.EventQueue.invokeLater(new Runnable() {
//
//            public void run() {
                final MultiTimeSeriesChart dialog = new MultiTimeSeriesChart(new javax.swing.JFrame( string ), false);
                dialog.addWindowListener(new java.awt.event.WindowAdapter() {

                    public void windowClosing(java.awt.event.WindowEvent e) {
                        dialog.setVisible(false);
                    }
                });
                Enumeration<TimeSeries> en = mrs.elements();
                while (en.hasMoreElements()) {
                    TimeSeries mr = en.nextElement();
                    dialog.initMessreihe( mr );
                    //System.out.println( mr.getStatisticData("> ") );
                }
                dialog.chartTitle = string;
                dialog.xLabel = x;
                dialog.yLabel = y;
                dialog.useLegende = b;
                dialog.statisticTextField.setText(comment);
                dialog.initChart();
                dialog.setTitle(string);
                dialog.setVisible(true);
//            }
//        });
    }
    
        public static MultiTimeSeriesChart open2(final Vector<TimeSeries> mrs, final String string, final String x, final String y, final boolean b , final String comment) {
//        java.awt.EventQueue.invokeLater(new Runnable() {
//
//            public void run() {
                final MultiTimeSeriesChart dialog = new MultiTimeSeriesChart(new javax.swing.JFrame( string ), false);
                dialog.addWindowListener(new java.awt.event.WindowAdapter() {

                    public void windowClosing(java.awt.event.WindowEvent e) {
                        dialog.setVisible(false);
                    }
                });
                Enumeration<TimeSeries> en = mrs.elements();
                while (en.hasMoreElements()) {
                    TimeSeries mr = en.nextElement();
                    dialog.initMessreihe( mr );
                    //System.out.println( mr.getStatisticData("> ") );
                }
                dialog.chartTitle = string;
                dialog.xLabel = x;
                dialog.yLabel = y;
                dialog.useLegende = b;
                dialog.statisticTextField.setText(comment);
                dialog.initChart();
                dialog.setTitle(string);
                // dialog.setVisible(true);
//            }
//        });
                return dialog;
    }

    public static Container openAndStore(final Vector<TimeSeries> mrs,
            final String string, final String x, final String y,
            final boolean b, final String folder, final String filename,
            String comment) {

                final MultiTimeSeriesChart dialog = new MultiTimeSeriesChart(new javax.swing.JFrame( string ), false);
                dialog.addWindowListener(new java.awt.event.WindowAdapter() {

                    public void windowClosing(java.awt.event.WindowEvent e) {
                        dialog.setVisible(false);
                    }
                });
                Enumeration<TimeSeries> en = mrs.elements();
                while (en.hasMoreElements()) {
                    TimeSeries mr = en.nextElement();
                    dialog.initMessreihe( mr );
                    //System.out.println( mr.getStatisticData("> ") );
                }
                dialog.chartTitle = string;
                dialog.xLabel = x;
                dialog.yLabel = y;
                dialog.useLegende = b;

                dialog.statisticTextField.setText( comment );

                dialog.initChart();
                dialog.setTitle(string);
                
                dialog.setVisible(true);
                File f2 = new File( folder);

                dialog.store( dialog.chart, f2, filename);

                TimeSeriesTable tab = new TimeSeriesTable();

                File f = new File( folder + "/" + filename + ".dat" );
                tab.setMessReihen( mrs );

                tab.writeToFile( f );

                return dialog.getContentPane();

    }

    public static void openNormalized(final Vector<TimeSeries> mrs, final String string, final String x, final String y, final boolean b) {
        java.awt.EventQueue.invokeLater(new Runnable() {

            public void run() {
                final MultiTimeSeriesChart dialog = new MultiTimeSeriesChart(new javax.swing.JFrame( string ), false);
                dialog.addWindowListener(new java.awt.event.WindowAdapter() {

                    public void windowClosing(java.awt.event.WindowEvent e) {
                        dialog.setVisible(false);
                    }
                });
                Enumeration<TimeSeries> en = mrs.elements();
                while (en.hasMoreElements()) {
                    TimeSeries mr = en.nextElement();
                    mr.normalize();
                    dialog.initMessreihe( mr );
                    //System.out.println( mr.getStatisticData("> ") );
                }
                dialog.chartTitle = string;
                dialog.xLabel = x;
                dialog.yLabel = y;
                dialog.useLegende = b;

                dialog.initChart();
                dialog.setTitle(string);
                dialog.setVisible(true);
            }
        });
    }
 
    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JPanel chartPanel;
    private javax.swing.JButton jButton1;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JPanel jPanel3;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JTextArea statisticTextField;

    public JTextArea getStatisticTextField() {
        return statisticTextField;
    }

    public void setStatisticTextField(JTextArea statisticTextField) {
        this.statisticTextField = statisticTextField;
    }
    // End of variables declaration//GEN-END:variables


    public void initMessreihe(TimeSeries mr) {
        // if ( this.normalizeY ) mr.normalizeY();
        if ( mr == null ) return;
        
        XYSeries series = new XYSeries(mr.getLabel());

        Vector<Double> x = mr.getXValues();
        Vector<Double> y = mr.getYValues();

        for (int i = 0; i < x.size(); i++) {
            series.add(x.elementAt(i), y.elementAt(i));
        }
        series.setDescription(mr.getLabel());
        this.addSerie(series);
    }

    public void addSerie(XYSeries serie) {
        dataset.addSeries(serie);
    }

    public void store(JFreeChart cp, File folder, String filename) {
        if (doStoreChart) {


//            File folder = LogFile.folderFile;
//            String fn = folder.getAbsolutePath() + File.separator + "images/Distribution_";
//            File file = null;
//            fn = fn + GeneralResultRecorder.currentSimulationLabel;


            String fn = filename;
            try {

                final File file1 = new File(folder.getAbsolutePath() + File.separator + fn + ".png");
                System.out.println("\n>>> Save as PNG Image - Filename: " + file1.getAbsolutePath()
                        + "; CP: "+ cp);
                            try {
                final ChartRenderingInfo info = new ChartRenderingInfo
                (new StandardEntityCollection());

                Thread.currentThread().sleep(1000);

                ChartUtilities.saveChartAsPNG(file1, chart, 600, 400, info);

                Thread.currentThread().sleep(1000);
            }
            catch (Exception e) {
                e.printStackTrace();
            }



//                File file = new File(folder.getAbsolutePath() + File.separator + fn + ".svg");
//                System.out.println(">>> Save as SVG Image - Filename: " + file.getAbsolutePath()
//                        + "; CP: "+ cp);
//
//
//                // Get a DOMImplementation and create an XML document
//                DOMImplementation domImpl =
//                        GenericDOMImplementation.getDOMImplementation();
//                Document document = domImpl.createDocument(null, "svg", null);
//
//                // Create an instance of the SVG Generator
//                SVGGraphics2D svgGenerator = new SVGGraphics2D(document);
//
//                // draw the chart in the SVG generator
//                cp.draw(svgGenerator, new Rectangle(800, 600));
//
//                // Write svg file
//                OutputStream outputStream = new FileOutputStream(file);
//                Writer out = new OutputStreamWriter(outputStream, "UTF-8");
//                svgGenerator.stream(out, true /* use css */);
//                outputStream.flush();
//                outputStream.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    boolean autoscale = true;
    
    public static boolean setDefaultRange = false;
    public ChartPanel cp = null;
    void initChart() {
        chartPanel.removeAll();
        cp = _createChartPanel();
        chartPanel.add(cp, BorderLayout.CENTER);
            
        
            NumberAxis xAxis = (NumberAxis)chart.getXYPlot().getDomainAxis();
            NumberAxis yAxis = (NumberAxis)chart.getXYPlot().getRangeAxis();
            
            if( setDefaultRange ) {
                xAxis.setRange(xRangDEFAULT_MIN, xRangDEFAULT_MAX);
                yAxis.setRange(yRangDEFAULT_MIN, yRangDEFAULT_MAX);
                
            }   
            else {
                xAxis.setAutoRange(autoscale);
                yAxis.setAutoRange(autoscale);
            }
            
            RefineryUtilities.centerFrameOnScreen(this);
            
    }

    public ChartPanel _createChartPanel() {
        JFreeChart chart2 = ChartFactory.createXYLineChart(
                chartTitle,
                xLabel,
                yLabel,
                dataset,
                PlotOrientation.VERTICAL,
                useLegende, // legende
                true,
                true);

        XYPlot plot = chart2.getXYPlot();
        plot.setBackgroundPaint( bgCOLOR );

        if ( dcSet == null ) {

            plot.getRenderer().setSeriesPaint(0, Color.BLACK);
            plot.getRenderer().setSeriesPaint(1, Color.blue);
            plot.getRenderer().setSeriesPaint(2, Color.orange);
            plot.getRenderer().setSeriesPaint(3, Color.RED);
            plot.getRenderer().setSeriesPaint(4, Color.gray);

            plot.getRenderer().setSeriesPaint(5, Color.BLACK);
            plot.getRenderer().setSeriesPaint(6, Color.blue);
            plot.getRenderer().setSeriesPaint(7, Color.orange);
            plot.getRenderer().setSeriesPaint(8, Color.RED);
            plot.getRenderer().setSeriesPaint(9, Color.gray);

        }
        else {

            int i = 0;
            for( Color c : dcSet ) {
                plot.getRenderer().setSeriesPaint(i,c);
                System.out.println( "Color: " + c);
                i++;
            }
        }

        XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) plot.getRenderer();

        BasicStroke s1 = new BasicStroke(
                    1.9f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND,
                    0.0f);
        
//        renderer.setSeriesStroke( 0, s1 );
//        renderer.setSeriesStroke( 1, s );
        renderer.setSeriesStroke( 8, s1 );
//        renderer.setSeriesStroke( 3, s );
//        renderer.setSeriesStroke( 4, s );
        

        if ( useMyStroke ) initStrokes( renderer );

        ChartPanel chartPanel = new ChartPanel(chart2);
        chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
        chart = chart2;
        return chartPanel;
    }
    
    static public boolean useMyStroke = false;
    static BasicStroke[] strokes= new BasicStroke[2];
    static int[] dt = null;
    
    public static void initStrokes( XYLineAndShapeRenderer renderer ) { 
            BasicStroke s0 = new BasicStroke( 1.5f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 0.0f);
            BasicStroke s1 = new BasicStroke( 1.5f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 1.0f, new float[] {2.0f, 4.0f}, 0.0f );
            strokes[0] = s1;
            strokes[1] = s0;
            
            for( int i=0; i < dt.length; i++ ) { 
                renderer.setSeriesStroke( i, strokes[ dt[i] ] );
            }
    }

    boolean normalizeY = false;

    void normalizeY() {
        this.normalizeY = true;
    }

        public static void open(final Vector<TimeSeries> mrs, final String string, final String x, final String y, final boolean b) {
        java.awt.EventQueue.invokeLater(new Runnable() {

            public void run() {
                final MultiTimeSeriesChart dialog = new MultiTimeSeriesChart(new javax.swing.JFrame( string ), false);
                dialog.addWindowListener(new java.awt.event.WindowAdapter() {

                    public void windowClosing(java.awt.event.WindowEvent e) {
                        dialog.setVisible(false);
                    }
                });
                Enumeration<TimeSeries> en = mrs.elements();
                while (en.hasMoreElements()) {
                    TimeSeries mr = en.nextElement();
                    dialog.initMessreihe( mr );
                    //System.out.println( mr.getStatisticData("> ") );
                }
                dialog.chartTitle = string;
                dialog.xLabel = x;
                dialog.yLabel = y;
                dialog.useLegende = b;

                dialog.setBackground(Color.lightGray);
                dialog.initChart();
                dialog.setTitle(string);
                
                
                dialog.setVisible(true);
            }
        });
    }

    public static void store(final TimeSeries[] mrs,
            final String string, final String x, final String y,
            final boolean b, final String folder, final String filename,
            String comment) {
        Vector<TimeSeries> r = new Vector<TimeSeries>();
        for( int i=0; i < mrs.length; i++) {
            r.add( mrs[i] );
        }
        store( r, string, x, y, b, folder, filename, comment);
    }


    public static void store(final Vector<TimeSeries> mrs,
            final String title, final String x, final String y,
            final boolean b, final String folder, final String filename,
            String comment) {

                final MultiTimeSeriesChart dialog = new MultiTimeSeriesChart(new javax.swing.JFrame( title ), false);
                dialog.addWindowListener(new java.awt.event.WindowAdapter() {

                    public void windowClosing(java.awt.event.WindowEvent e) {
                        dialog.setVisible(false);
                    }
                });

                Enumeration<TimeSeries> en = mrs.elements();
                int i = 0;
                while (en.hasMoreElements()) {
                    TimeSeries mr = en.nextElement();
                    dialog.initMessreihe( mr );
                    // System.out.println( "... init reihe="+i );
                    i++;
                }
                dialog.chartTitle = title;
                dialog.xLabel = x;
                dialog.yLabel = y;
                dialog.useLegende = b;

                dialog.statisticTextField.setText( comment );

                dialog.initChart();
                dialog.setTitle(title);
                //dialog.setVisible(true);
                File f2 = new File( folder);

                dialog.store( dialog.chart, f2, filename);
                
                System.out.println( folder + "\t" + filename );
                TimeSeriesTable tab = new TimeSeriesTable();
                tab.fill_UP_VALUE = 0.0;
                
                if ( mrs.size() > 0 ) {
                    File f = new File( folder + "/" + "TAB_" + filename + ".dat" );
                    tab.setMessReihen( mrs );
                    System.out.println(mrs.size() + " reihen ...");
                    tab.writeToFile( f );
                }         

    }


    public ChartPanel createChartPanel() {
        JFreeChart chart = ChartFactory.createXYLineChart(
                chartTitle,
                xLabel,
                yLabel,
                dataset,
                PlotOrientation.VERTICAL,
                useLegende, // legende
                true,
                true);
        chart.getXYPlot().setForegroundAlpha(0.75f);
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
        return chartPanel;
    }

    static Color[] dcSet = null;

    public void _setDefaultColors(Color[] dcSet1) {
        dcSet = dcSet1;
        initChart();
    }

     
}
