package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;

public class SimpleDynamoProvider extends ContentProvider {

	static final String REMOTE_PORT0 = "5554";
	static final String REMOTE_PORT1 = "5556";
	static final String REMOTE_PORT2 = "5558";
	static final String REMOTE_PORT3 = "5560";
	static final String REMOTE_PORT4 = "5562";
	static final int SERVER_PORT = 10000;
	static String prevPort1 = null;
	static String prevPort2 = null;
	static String prevPort3 = null;
	static String myPort = null;
	static String myPortHash = null;
	static String[] remoteHosts = null;
	static ArrayList<String> files = null;
	static ArrayList<String> filesReplica1 = null;
	static ArrayList<String> filesReplica2 = null;

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				while(true){
					Socket clientSocket = serverSocket.accept();
					BufferedReader in = new BufferedReader(
							new InputStreamReader(clientSocket.getInputStream()));
					String line = in.readLine();
					BufferedWriter out = new BufferedWriter(
							new OutputStreamWriter(clientSocket.getOutputStream()));
					String[] tmp = line.split("@");

					if (tmp[0].equals("set")) {
						if(!files.isEmpty()){
							String response = "";
							for (String file: files){
								InputStreamReader isr = new InputStreamReader(getContext().openFileInput(file));
								BufferedReader br = new BufferedReader(isr);
								response = response + file + "@" + br.readLine() + "@";
								br.close();
							}
							out.write(response.substring(0, response.length()-1) + "\n");
							out.flush();
						} else{
							out.write("Response" + "\n");
							out.flush();
						}
					} else if (tmp[0].equals("insert")) {
						try {
							FileOutputStream fos = getContext().openFileOutput(tmp[1], Context.MODE_PRIVATE);
							fos.write(tmp[2].getBytes());
							fos.close();
							if(!files.contains(tmp[1])) {
								files.add(tmp[1]);
							}
							out.write("Response"+"\n");
							out.flush();
						} catch (IOException e) {
							e.printStackTrace();
						}
					} else if (tmp[0].equals("delete")) {
						try {
							if(files.contains(tmp[1])) {
								files.remove(tmp[1]);
							}
							out.write("Response"+"\n");
							out.flush();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					in.close();
					clientSocket.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... msgs) {
			try {
				String remoteHost = Integer.toString(Integer.parseInt(msgs[0]) * 2);
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remoteHost));
				OutputStreamWriter o = new OutputStreamWriter(socket.getOutputStream());
				BufferedWriter bw = new BufferedWriter(o);
				String msg = msgs[1];
				bw.write(msg);
				bw.flush();
				InputStreamReader i = new InputStreamReader(socket.getInputStream());
				BufferedReader br = new BufferedReader(i);
				String response = null;
				int wait = 0;
				while (response == null) {
					wait++;
					if(wait > 1000){
						throw new SocketTimeoutException();
					}
					response = br.readLine();
				}
				if ((response != null) && (!response.equals("Response"))) {
					String[] tmp = response.split("@");
					for (int j = 0; j < tmp.length - 1; j = j + 2) {
						FileOutputStream fos = getContext().openFileOutput(tmp[j], Context.MODE_PRIVATE);
						fos.write(tmp[j + 1].getBytes());
						fos.close();
						files.add(tmp[j]);
					}
				}
				socket.close();
			} catch(SocketTimeoutException e) {
				e.printStackTrace();
				this.cancel(true);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("@")) {

		}else if(selection.equals("*")){
			for (String file : files) {
				files.remove(file);
			}
		}else {
			if(files.contains(selection)) {
				files.remove(selection);
			}
			for (String remoteHost: remoteHosts) {
				if(!remoteHost.equals(myPort)) {
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, remoteHost, "delete" + "@" + selection + "\n");
				}
			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String fn = null;
		String value = null;
		for (String key: values.keySet()){
			if (key.equals("key")){
				fn = values.get(key).toString();
			} else {
				value = values.get(key).toString();
			}
		}

		try {
			FileOutputStream fos = getContext().openFileOutput(fn, Context.MODE_PRIVATE);
			fos.write(value.getBytes());
			fos.close();
			if(!files.contains(fn)) {
				files.add(fn);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (String remoteHost: remoteHosts) {
			if(!remoteHost.equals(myPort)) {
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, remoteHost, "insert" + "@" + fn + "@" + value + "\n");
			}
		}
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		ServerSocket socket = null;
		try {
			socket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, socket);
		} catch (IOException e) {
			e.printStackTrace();
		}

		files = new ArrayList<String>();
		filesReplica1 = new ArrayList<String>();
		filesReplica2 = new ArrayList<String>();
		remoteHosts = new String[] {REMOTE_PORT4, REMOTE_PORT1, REMOTE_PORT0, REMOTE_PORT2, REMOTE_PORT3};

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr)));

		try {
			myPortHash = genHash(myPort);
			for(int i=0; i<remoteHosts.length; i++){
				if(remoteHosts[i].equals(myPort)){
					int j = (i == 0) ? remoteHosts.length-1 : i-1;
					prevPort1 = remoteHosts[j];
					j = (j == 0) ? remoteHosts.length-1 : j-1;
					prevPort2 = remoteHosts[j];
					j = (j == 0) ? remoteHosts.length-1 : j-1;
					prevPort3 = remoteHosts[j];
					break;
				}
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, prevPort1, "set" + "@" + myPort + "\n");
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		Cursor mc = new MatrixCursor(new String[]{"key", "value"});
		if(selection.equals("*")){
			for(String file : files){
				try {
					InputStreamReader isr = new InputStreamReader(getContext().openFileInput(file));
					BufferedReader br = new BufferedReader(isr);
					((MatrixCursor) mc).newRow().add("key", file).add("value", br.readLine());
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		else if(selection.equals("@")){
			for(String file : files){
				try {
					String fileHash = genHash(file);
					if((remoteHosts[0].equals(myPort)) && ((genHash(prevPort1).compareTo(fileHash) < 0) || (myPortHash.compareTo(fileHash) >= 0))) {
						InputStreamReader isr = new InputStreamReader(getContext().openFileInput(file));
						BufferedReader br = new BufferedReader(isr);
						((MatrixCursor) mc).newRow().add("key", file).add("value", br.readLine());
						br.close();
					} else if ((genHash(prevPort1).compareTo(fileHash) < 0) && (myPortHash.compareTo(fileHash) >= 0)) {
						InputStreamReader isr = new InputStreamReader(getContext().openFileInput(file));
						BufferedReader br = new BufferedReader(isr);
						((MatrixCursor) mc).newRow().add("key", file).add("value", br.readLine());
						br.close();
					} else if ((remoteHosts[0].equals(prevPort1)) && ((genHash(prevPort2).compareTo(fileHash) < 0) || (genHash(prevPort1).compareTo(fileHash) >= 0))) {
						InputStreamReader isr = new InputStreamReader(getContext().openFileInput(file));
						BufferedReader br = new BufferedReader(isr);
						((MatrixCursor) mc).newRow().add("key", file).add("value", br.readLine());
						br.close();
					} else if ((genHash(prevPort2).compareTo(fileHash) < 0) && (genHash(prevPort1).compareTo(fileHash) >= 0)) {
						InputStreamReader isr = new InputStreamReader(getContext().openFileInput(file));
						BufferedReader br = new BufferedReader(isr);
						((MatrixCursor) mc).newRow().add("key", file).add("value", br.readLine());
						br.close();
					} else if ((remoteHosts[0].equals(prevPort2)) && ((genHash(prevPort3).compareTo(fileHash) < 0) || (genHash(prevPort2).compareTo(fileHash) >= 0))) {
						InputStreamReader isr = new InputStreamReader(getContext().openFileInput(file));
						BufferedReader br = new BufferedReader(isr);
						((MatrixCursor) mc).newRow().add("key", file).add("value", br.readLine());
						br.close();
					} else if ((genHash(prevPort3).compareTo(fileHash) < 0) && (genHash(prevPort2).compareTo(fileHash) >= 0)) {
						InputStreamReader isr = new InputStreamReader(getContext().openFileInput(file));
						BufferedReader br = new BufferedReader(isr);
						((MatrixCursor) mc).newRow().add("key", file).add("value", br.readLine());
						br.close();
					}
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}catch (IOException e) {
					e.printStackTrace();
				}
			}
		}else {
			try {
				InputStreamReader isr = new InputStreamReader(getContext().openFileInput(selection));
				BufferedReader br = new BufferedReader(isr);
				((MatrixCursor) mc).newRow().add("key", selection).add("value", br.readLine());
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return mc;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
