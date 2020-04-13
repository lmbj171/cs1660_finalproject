package finalproject.finalproject;

import javax.swing.*;
import java.awt.FlowLayout;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.awt.*;
import java.awt.event.*;

import com.spotify.dataproc.DataprocHadoopRunner;
import com.spotify.dataproc.Job;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class App extends JFrame implements ActionListener
{
	static JFrame frame;
	static JPanel topPanel;
	static JPanel bottomPanel;
	static JPanel panel;
	static JPanel resultsPanel;
	static JButton chooseButton;
	static JButton constructIndices;
	static JButton topN;
	static JButton termSearch;
	static JButton showIIResults;
	static JCheckBox c1, c2, c3, c4;
	static JLabel fileSelection;
	static JLabel statusLabel;
	static JLabel selected;
	static boolean c1Checked, c2Checked, c3Checked, c4Checked = false;
	final static JFileChooser fileChooser = new JFileChooser("C:\\Users\\Linn\\Desktop\\");
	static JTextArea textArea = new JTextArea("Load My Engine");
	static JTextArea resultsField = new JTextArea(25, 50);
	static JScrollPane scroller = new JScrollPane(resultsField);
	static String results = ""; 
	static File[] files = null;
	
	public static void main(String[] args)
	{
		new App();
	}
	
    public App()
	{
       frame = new JFrame("Final Project");
       frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
       frame.setSize(1000,1000);
	   frame.setLayout(new FlowLayout());
	   frame.setLocationRelativeTo(null);
	   
	   textArea.setEditable(false);
	   textArea.setFont(new Font("Serif", Font.BOLD, 35));
	   textArea.setBounds(0,0, 0, 0);
	   frame.add(textArea);
	  
	   //create buttons
	   chooseButton = new JButton("Choose File");
	   constructIndices = new JButton("Construct Inverted Indicies");
	   topN = new JButton("Top-N");
	   termSearch = new JButton("Term Search");
	   showIIResults = new JButton("Show Inverted Indicies Results");
	   
	   //add button listeners
	   chooseButton.addActionListener(this);
	   constructIndices.addActionListener(this);
	   topN.addActionListener(this);
	   termSearch.addActionListener(this);
	   showIIResults.addActionListener(this);

	   topPanel = new JPanel();
	   topPanel.add(chooseButton);
	   topPanel.add(constructIndices);
	   frame.add(topPanel);
	   
	   bottomPanel = new JPanel();
	   fileSelection = new JLabel("Select file(s):");
	   selected = new JLabel("No File Selected");
	   
	   c1 = new JCheckBox("Shakespeare");
	   c2 = new JCheckBox("Hugo");
	   c3 = new JCheckBox("Tolstoy");
	   c4 = new JCheckBox("ALL");
	    
	   bottomPanel.add(fileSelection);
	   bottomPanel.add(c1);
	   bottomPanel.add(c2);
	   bottomPanel.add(c3);
	   bottomPanel.add(c4);
	   bottomPanel.add(selected);
	   statusLabel = new JLabel("");
	   
	   frame.add(bottomPanel);
	   
	   panel = new JPanel();
	   panel.add(topN);
	   panel.add(termSearch);
	   panel.add(showIIResults);
	   frame.add(panel);
	   panel.setVisible(false);
       
       //add checkbox listeners
       c1.addItemListener(new CustomItemListener());
       c2.addItemListener(new CustomItemListener());
       c3.addItemListener(new CustomItemListener());
       c4.addItemListener(new CustomItemListener());
       
       resultsPanel = new JPanel();
       
       resultsField.setEditable(false);
   	   resultsField.setLineWrap(true);
       
   	   scroller.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
       scroller.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
       
       resultsPanel.add(scroller);

       frame.add(resultsPanel);
       resultsPanel.setVisible(false);
       
	   frame.setVisible(true);
    }
    
    public void actionPerformed(ActionEvent e)
    {      	
    	if(e.getSource() == chooseButton)
    	{
    		fileChooser.setMultiSelectionEnabled(true);
			int returnVal = fileChooser.showOpenDialog(this);
			if (returnVal == JFileChooser.APPROVE_OPTION) 
			{
				files = fileChooser.getSelectedFiles();
				selected.setText("");
				int t = 0;
				
				while (t++ < files.length) 
					selected.setText(selected.getText() + " " + files[t - 1].getName());
			}
			
			try {
				uploadFiles();
			} catch (IOException e2) {
				e2.printStackTrace();
			}
       	}
    	else if(e.getSource() == constructIndices)   	
    	{
    		try {
				uploadToArgsFolder();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
    		
			List<String> arguments = new ArrayList<String>();
			String path = "gs://dataproc-staging-us-280861026343-wpsvpkis/";
    		ArrayList<String> params = getParams();

    		if(!c4Checked) 
    		{
        		arguments.add("gs://dataproc-staging-us-280861026343-wpsvpkis/args");
    		}
    		else if(c4Checked)
    		{
    			arguments.add("gs://dataproc-staging-us-280861026343-wpsvpkis/input");
    		}
		   		
		    arguments.add("gs://dataproc-staging-us-280861026343-wpsvpkis/output");
		    String[] jar = {"gs://dataproc-staging-us-280861026343-wpsvpkis/JAR/InvertedIndex.jar"};
		
			final String project = "arched-shuttle-273617";
			final String cluster = "cluster-c67a";
					
			DataprocHadoopRunner hadoopRunner = DataprocHadoopRunner.builder("arched-shuttle-273617", "cluster-c67a").build();
				
		   	Job job = Job.builder()
		   	    .setMainClass("InvertedIndex")
		   	    .setArgs(arguments)
		   	    .setShippedJars(jar)
		   	    .setShippedFiles(null)
		   	    .createJob();
		
		   	try 
		   	{
				hadoopRunner.submit(job);
				System.out.println("Job submitted successfully");
			} 
		   	catch (IOException e1) {
				e1.printStackTrace();
			}
			
		   	
			jobCompleted();
    	}
    	else if(e.getSource() == topN)
    	{
    		JOptionPane.showMessageDialog(null, "Method Not Implemented");
    	}
    	else if(e.getSource() == termSearch)
    	{
    		JOptionPane.showMessageDialog(null, "Method Not Implemented");
    	}
    	else if(e.getSource() == showIIResults)
    	{
    		try {
				getResults();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
    		showResults(results);
    	}
    	
    }
    
    public static void uploadFiles() throws IOException
    {
    	final String project = "arched-shuttle-273617";
    	final String bucketName = "dataproc-staging-us-280861026343-wpsvpkis";
    	String filename = "";
    	String path = "";
    	
    	for(int i = 0; i < files.length; i++)
    	{
    		filename = files[i].getName();
    		path = files[i].getPath().replace('\\', '/');
    		final com.google.cloud.storage.Storage storage = StorageOptions.newBuilder().setProjectId(project).build().getService();
    	    BlobId blobId = BlobId.of(bucketName, filename);
    	    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    	    storage.create(blobInfo, Files.readAllBytes(Paths.get(path)));
    	}
    }
    
    public static void uploadToArgsFolder() throws IOException
    {   	
    	final String project = "arched-shuttle-273617";
    	final String bucketName = "dataproc-staging-us-280861026343-wpsvpkis";
    	ArrayList<String> params = new ArrayList<String>();
    	
		final com.google.cloud.storage.Storage storage = StorageOptions.newBuilder().setProjectId(project).build().getService();
		Bucket bucket = storage.get(bucketName);

    	if (files != null)
    	{
    		//copy into args folder
    		for(File f : files)
    		{
    			BlobId blobId = BlobId.of(bucketName, f.getName());
    			copy(blobId, bucketName, "" + "args/" + blobId.getName(), false);
    		}
    	}
    	
    	params = getParams(); 
    	for (String p : params)
    	{
    		BlobId blobId = BlobId.of(bucketName, p);
			copy(blobId, bucketName, "" + "args/" + blobId.getName(), false);
    	}
    }
    
    public static void copy(BlobId sourceBlobId, String destinationBucket, String destinationPath, boolean deleteSource) 
    {
    	final String project = "arched-shuttle-273617";
		final com.google.cloud.storage.Storage storage = StorageOptions.newBuilder().setProjectId(project).build().getService();
    	  Storage.CopyRequest copyRequest = new Storage.CopyRequest.Builder()
    	      .setSource(sourceBlobId)
    	      .setTarget(BlobId.of(destinationBucket, destinationPath))
    	      .build();
    	  Blob destinationBlob = storage.copy(copyRequest).getResult();
    }
    
    public static void jobCompleted()
    {
    	panel.setVisible(true);
    }
    
    public static void getResults() throws IOException
    {    	
    	final String project = "arched-shuttle-273617";
		final String bucketName = "dataproc-staging-us-280861026343-wpsvpkis";
		final String output = "output.txt";
    	
	  	final com.google.cloud.storage.Storage storage = StorageOptions.newBuilder().setProjectId(project).build().getService();
	    Bucket bucket = storage.get(bucketName);
	    Page<Blob> blobs =
	            bucket.list(
	                Storage.BlobListOption.prefix(output),
	                Storage.BlobListOption.currentDirectory());

        for (Blob blob : blobs.iterateAll()) 
        {
          System.out.println(blob.getName());
          byte[] content = blob.getContent();
          results = new String(content);
        }
    }
    
    public static void showResults(String r)
    {
    	resultsField.setText(r);
    	scroller.setVisible(true);
        resultsPanel.setVisible(true);
    }
    
    public static ArrayList<String> getParams()
    {
    	ArrayList<String> params = new ArrayList<String>();
    	ArrayList<String> allSelected = new ArrayList<String>();
    	
    	if (c1Checked){
    		params.add("Shakespeare.tar.gz");
    	}
    	if (c2Checked){
    		params.add("Hugo.tar.gz");
    	}
    	if (c3Checked){
    		params.add("Tolstoy.tar.gz");
    	}
    	if (c4Checked) {
    		allSelected.add("Shakespeare.tar.gz");
    		allSelected.add("Hugo.tar.gz");
    		allSelected.add("Tolstoy.tar.gz");
    		return allSelected;
    	}
    	
    	return params;
    }
    
    static class CustomItemListener implements ItemListener
    {
    	public void itemStateChanged(ItemEvent e) 
    	{    		
    		if(e.getSource() == c1){
    			c1Checked = true;
    			c4.setSelected(false);
    		}
    		if(e.getSource() == c2){
    			c2Checked = true;
    			c4.setSelected(false);
    		}
    		if(e.getSource() == c3){
    			c3Checked = true;
    			c4.setSelected(false);
    		}
    		if(e.getSource() == c4){
    			c4Checked = true;
    			c1.setSelected(false);
    			c2.setSelected(false);
    			c3.setSelected(false);
    		}
    	}
    	
    }
}    