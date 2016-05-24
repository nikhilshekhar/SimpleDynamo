package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Hashtable;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
    static final String[] REMOTE_PORT = {"11124","11112","11108","11116","11120"};
    static final String[] NODES= {"5562","5556","5554","5558","5560"};
    static Hashtable<String,String> queryResults = new Hashtable<String,String>();
    static MatrixCursor cursorWithStarMessages = new MatrixCursor(new String[]{"key","value"});

    ArrayList<String> recordOfData = new ArrayList<String>(7);

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        if(selection.compareToIgnoreCase("*")==0) {
            String[] files = getContext().fileList();
            for (String file : files) {
                getContext().deleteFile(file);
            }
//            new deleteQueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        } else if(selection.compareToIgnoreCase("@")==0){

            String[] files = getContext().fileList();

            for(String file: files){
                getContext().deleteFile(file);
            }
            new deleteQueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }else {
            getContext().deleteFile(selection);
        }

        return 0;
	}

    private class deleteQueryClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            try {
                String msgToSend = "DeleteTask" + ":";
                if (!REMOTE_PORT0.equals(recordOfData.get(0))) {
                    Socket socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT0));
                    socket0.getOutputStream().write(msgToSend.getBytes());
                    socket0.getOutputStream().flush();
                    socket0.close();
                    Log.v(TAG, "Message sent from REMOTE_PORT0:" + REMOTE_PORT0);
                }

                if (!REMOTE_PORT1.equals(recordOfData.get(0))) {
                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT1));
                    socket1.getOutputStream().write(msgToSend.getBytes());
                    socket1.getOutputStream().flush();
                    socket1.close();
                    Log.v(TAG, "Message sent from REMOTE_PORT1:" + REMOTE_PORT1);
                }

                if (!REMOTE_PORT2.equals(recordOfData.get(0))) {
                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT2));
                    socket2.getOutputStream().write(msgToSend.getBytes());
                    socket2.getOutputStream().flush();
                    socket2.close();
                    Log.v(TAG, "Message sent from REMOTE_PORT2:" + REMOTE_PORT2);
                }

                if (!REMOTE_PORT3.equals(recordOfData.get(0))) {
                    Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT3));
                    socket3.getOutputStream().write(msgToSend.getBytes());
                    socket3.getOutputStream().flush();
                    socket3.close();
                    Log.v(TAG, "Message sent from REMOTE_PORT3:" + REMOTE_PORT3);
                }

                if (!REMOTE_PORT4.equals(recordOfData.get(0))) {
                    Socket socket4 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT4));
                    socket4.getOutputStream().write(msgToSend.getBytes());
                    socket4.getOutputStream().flush();
                    socket4.close();
                    Log.v(TAG, "Message sent from REMOTE_PORT4:" + REMOTE_PORT4);
                }

                Log.e(TAG, "Sending message:" + msgToSend);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
        try {
            //        portNumber:NodeId:hashedNode:nextNode:nextToNextNode
            //         0            1      2         3         4

            String destNode = null;
            try {
                String hashNewkey = genHash(values.getAsString("key"));
                if(genHash(NODES[0]).compareTo(hashNewkey) >=0 || genHash(NODES[4]).compareTo(hashNewkey)<0){
                    destNode = NODES[0];
                }else if(genHash(NODES[1]).compareTo(hashNewkey)>=0 && genHash(NODES[0]).compareTo(hashNewkey)<0 ){
                    destNode = NODES[1];
                }else if(genHash(NODES[2]).compareTo(hashNewkey)>=0 && genHash(NODES[1]).compareTo(hashNewkey)<0 ){
                    destNode = NODES[2];
                }else if(genHash(NODES[3]).compareTo(hashNewkey)>=0 && genHash(NODES[2]).compareTo(hashNewkey)<0 ){
                    destNode = NODES[3];
                }else if(genHash(NODES[4]).compareTo(hashNewkey)>=0 && genHash(NODES[3]).compareTo(hashNewkey)<0 ){
                    destNode = NODES[4];
                }

            }catch(Exception e){
                Log.e(TAG,e.getMessage());
            }


            Integer portId = 0;
            if(destNode.compareToIgnoreCase("5554")==0){
                portId = 11108;
            }else if(destNode.compareToIgnoreCase("5556")==0){
                portId = 11112;
            }else if(destNode.compareToIgnoreCase("5558")==0){
                portId = 11116;
            }else if(destNode.compareToIgnoreCase("5560")==0){
                portId = 11120;
            }
            else if(destNode.compareToIgnoreCase("5562")==0){
                portId = 11124;
            }

            if(destNode.equals(recordOfData.get(1))){
                //Write on current node
                FileOutputStream fos = getContext().openFileOutput(values.get("key").toString(), Context.MODE_PRIVATE);
                fos.write(values.getAsString("value").getBytes());
                fos.close();

            }
            else{
                //Write on the correct node
                new insertReplicateUpdate().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,portId.toString(),values.get("key").toString(),values.get("value").toString(),"insert");
            }

            //Replicate on two other nodes as well
            new insertReplicateUpdate().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,portId.toString(),values.get("key").toString(),values.get("value").toString(),"replicate");

        }catch (Exception e){
            e.printStackTrace();
        }
        return uri;
	}

	@Override
	public boolean onCreate() {

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		String hashedNode = null;
		try{
			hashedNode = genHash(portStr);
		}catch(Exception e){
			e.printStackTrace();
		}

//        portNumber:NodeId:hashedNode:nextNode:nextToNextNode
//         0            1      2         3         4

        int index= Arrays.asList(NODES).indexOf(portStr);
		recordOfData.add(0,myPort);
		recordOfData.add(1,portStr);
		recordOfData.add(2,hashedNode);
		recordOfData.add(3, NODES[(index + 1) % 5]);
		recordOfData.add(4, NODES[(index + 2) % 5]);

		try{
			Log.v(TAG, "Hash of  5554:" + genHash("5554"));//33d6357cfaaf0f72991b0ecd8c56da066613c089
			Log.v(TAG,"Hash of  5556:"+genHash("5556"));//208f7f72b198dadd244e61801abe1ec3a4857bc9
			Log.v(TAG,"Hash of  5558:"+genHash("5558"));//abf0fd8db03e5ecb199a9b82929e9db79b909643
			Log.v(TAG,"Hash of  5560:"+genHash("5560"));//c25ddd596aa7c81fa12378fa725f706d54325d12
			Log.v(TAG,"Hash of  5562:"+genHash("5562"));//177ccecaec32c54b82d5aaafc18a2dadb753e3b1
		}catch (Exception e){
			e.printStackTrace();
		}

		try {
			ServerSocket serverSocket = new ServerSocket(10000);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

        //Since node is recovering from failure, send the message to all nodes
        String[] files=getContext().fileList();
        if(files!=null && files.length>0){
            for(String file: files){
                getContext().deleteFile(file);
            }
            new RecoveryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,portStr);
        }

		return false;
	}

    // //Recovery task and insert task
    private class RecoveryClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Socket socket;
            OutputStream os;
            PrintWriter pw;
            for(String port: REMOTE_PORT){
                Integer portId = 0;
                if (msgs[0].compareToIgnoreCase("5554") == 0) {
                    portId = 11108;
                } else if (msgs[0].compareToIgnoreCase("5556") == 0) {
                    portId = 11112;
                } else if (msgs[0].compareToIgnoreCase("5558") == 0) {
                    portId = 11116;
                } else if (msgs[0].compareToIgnoreCase("5560") == 0) {
                    portId = 11120;
                } else if (msgs[0].compareToIgnoreCase("5562") == 0) {
                    portId = 11124;
                }

                if(!port.equals(portId)) {
                    try {
                        String msgToSend = "RecoveryTask" + ":" + msgs[0];
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(port));
                        os = socket.getOutputStream();
                        pw = new PrintWriter(os, true);
                        pw.println(msgToSend);
                        pw.flush();
                        socket.close();
                    } catch (Exception e) {
                        Log.e(TAG,e.getMessage());
                    }
                }
            }

            return null;
        }
    }

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */


            //        portNumber:NodeId:hashedNode:nextNode:nextToNextNode
            //         0            1      2         3         4
			try {
				while(true) {
					Socket serverSocketConnection = serverSocket.accept();
					BufferedReader in = new BufferedReader(new InputStreamReader(serverSocketConnection.getInputStream()));
					String p = in.readLine();
					serverSocketConnection.close();
					String[] partsOfMessageReceived = p.split(":");
                    String msgValue = "";
                    BufferedInputStream bis;
                    int temp;

					if (partsOfMessageReceived[0].compareToIgnoreCase("RecoveryTask") == 0) {

                        String[] files=getContext().fileList();
                        String updateMsg="";
                        for(String file: files){

                            String coordinator = null;
                            try {
                                String hashNewkey = genHash(file);
                                if(genHash(NODES[0]).compareTo(hashNewkey) >=0 || genHash(NODES[4]).compareTo(hashNewkey)<0){
                                    coordinator = NODES[0];
                                }else if(genHash(NODES[1]).compareTo(hashNewkey)>=0 && genHash(NODES[0]).compareTo(hashNewkey)<0 ){
                                    coordinator = NODES[1];
                                }else if(genHash(NODES[2]).compareTo(hashNewkey)>=0 && genHash(NODES[1]).compareTo(hashNewkey)<0 ){
                                    coordinator = NODES[2];
                                }else if(genHash(NODES[3]).compareTo(hashNewkey)>=0 && genHash(NODES[2]).compareTo(hashNewkey)<0 ){
                                    coordinator = NODES[3];
                                }else if(genHash(NODES[4]).compareTo(hashNewkey)>=0 && genHash(NODES[3]).compareTo(hashNewkey)<0 ){
                                    coordinator = NODES[4];
                                }
                            }catch(Exception e){
                                Log.e(TAG,e.getMessage());
                            }

                            String firstReplica,secondReplica;

                            if(partsOfMessageReceived[1].equals("5562")){
                                firstReplica = "5560";
                            }else if(partsOfMessageReceived[1].equals("5556")){
                                firstReplica = "5562";
                            } else if(partsOfMessageReceived[1].equals("5554")){
                                firstReplica = "5556";
                            } else if(partsOfMessageReceived[1].equals("5558")){
                                firstReplica = "5554";
                            }else {
                                firstReplica = "5558";
                            }

                            if(partsOfMessageReceived[1].equals("5562")){
                                secondReplica = "5558";
                            } else if(partsOfMessageReceived[1].equals("5556")){
                                secondReplica = "5560";
                            } else if(partsOfMessageReceived[1].equals("5554")){
                                secondReplica = "5562";
                            } else if(partsOfMessageReceived[1].equals("5558")){
                                secondReplica = "5556";
                            } else{
                                secondReplica = "5554";
                            }

                            if(coordinator.equals(partsOfMessageReceived[1]) ||coordinator.equals(firstReplica) ||coordinator.equals(secondReplica)) {
                                msgValue = "";
                                try {
                                    bis = new BufferedInputStream(getContext().openFileInput(file));
                                    while ((temp = bis.read()) != -1) {
                                        msgValue += (char) temp;
                                    }
                                    bis.close();
                                } catch (Exception e) {
                                    Log.e(TAG, e.getMessage());
                                }
                                updateMsg+=file+"#"+msgValue+":";
                            }
                        }

                        callUpdateClientTask(partsOfMessageReceived[1], updateMsg);
					}else if(partsOfMessageReceived[0].compareToIgnoreCase("UpdateTask") == 0){
                        //        portNumber:NodeId:hashedNode:nextNode:nextToNextNode
                        //         0            1      2         3         4

                        for(String msg: partsOfMessageReceived) {
                            if(msg.contains("#")) {
                                FileOutputStream fos = getContext().openFileOutput(msg.split("#")[0], Context.MODE_PRIVATE);
                                fos.write(msg.split("#")[1].getBytes());
                                fos.close();
                            }
                        }
                    }else if(partsOfMessageReceived[0].compareToIgnoreCase("InsertTask") == 0){
                        try {
                            FileOutputStream fos = getContext().openFileOutput(partsOfMessageReceived[1], Context.MODE_PRIVATE);
                            fos.write(partsOfMessageReceived[2].getBytes());
                            fos.close();
                        }catch(IOException e){
                            Log.e(TAG,e.getMessage());
                        }

					}else if(partsOfMessageReceived[0].compareToIgnoreCase("InsertReplicateTask") == 0){
                        try {
                            FileOutputStream fos = getContext().openFileOutput(partsOfMessageReceived[1], Context.MODE_PRIVATE);
                            fos.write(partsOfMessageReceived[2].getBytes());
                            fos.close();
                        }catch(Exception e){
                            Log.e(TAG,e.getMessage());
                        }

                    }else if(partsOfMessageReceived[0].compareToIgnoreCase("QueryTask") == 0){
                        MatrixCursor resultCursor = new MatrixCursor(new String[]{"key","value"});
                        try {
                            bis = new BufferedInputStream(getContext().openFileInput(partsOfMessageReceived[1]));
                            while ((temp = bis.read()) != -1) {
                                msgValue += (char) temp;
                            }
                            bis.close();
                            resultCursor.addRow(new String[]{partsOfMessageReceived[1], msgValue});
                        }catch(Exception e){
                            Log.e(TAG,e.getMessage());
                        }
                        if(resultCursor.moveToFirst()){
                            String msgToSend = "ReturnQueryTask" + ":" + resultCursor.getString(resultCursor.getColumnIndex("key")) + ":" + resultCursor.getString(resultCursor.getColumnIndex("value")) + ":" + partsOfMessageReceived[2] + ":";
							joinTask(msgToSend);
						}else{

                        }

					}else if(partsOfMessageReceived[0].compareToIgnoreCase("ReturnQueryTask")==0){
                        queryResults.put(partsOfMessageReceived[1],partsOfMessageReceived[2]);

					}else if(partsOfMessageReceived[0].compareToIgnoreCase("StarQuery")==0){
						if(!partsOfMessageReceived[1].equals(recordOfData.get(0))){
							Uri uri = new Uri.Builder().scheme("content").authority("edu.buffalo.cse.cse486586.simpledht.provider").build();
							Cursor returnValues = query(uri, null, "@", null, null);
							String rowList="";
							if (returnValues != null && returnValues.getCount() > 0) {
								if (returnValues.moveToFirst()) {
									do {
										rowList += returnValues.getString(returnValues.getColumnIndex("key")) + "#" + returnValues.getString(returnValues.getColumnIndex("value")) + ":";
									} while (returnValues.moveToNext());
								}
								callReturnStarQueryClientTask(partsOfMessageReceived[1], rowList);
							}

						}
					}else if(partsOfMessageReceived[0].compareToIgnoreCase("ReturnStarQuery")==0){

                        //Might need a change
						for(int i=1;i<partsOfMessageReceived.length;i++){
							if(partsOfMessageReceived[i].contains("#")){
								cursorWithStarMessages.addRow(new String[]{partsOfMessageReceived[i].split("#")[0], partsOfMessageReceived[i].split("#")[1]});
							}
						}
					}else if(partsOfMessageReceived[0].compareToIgnoreCase("StarDelete")==0){
                        String[] files = getContext().fileList();
                        for(String file: files){
                            getContext().deleteFile(file);
                        }
					}else if(partsOfMessageReceived[0].compareToIgnoreCase("DeleteTask")==0){
                        getContext().deleteFile(partsOfMessageReceived[1]);
                    }
				}
			}catch (Exception e){
				e.printStackTrace();
			}
			return null;
		}

		protected void joinTask(String p){
			String[] partsOfMessageReceived = p.split(":");
			if (p != null){
                if(partsOfMessageReceived[0].compareToIgnoreCase("ReturnQueryTask") == 0){
					new starQueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, partsOfMessageReceived[3],partsOfMessageReceived[1],partsOfMessageReceived[2]);
				}
			}
		}
	}

    protected  void callReturnStarQueryClientTask(String partOfMessage,String rowList){
        new queryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, partOfMessage, rowList);
    }

    private class queryClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Socket socket = null;
            DataOutputStream writeOutputStream = null;
            if(msgs.length==3) {
                String msgToSend = "QueryTask" + ":" + msgs[1] + ":" + msgs[2] + ":";
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0]));
                    writeOutputStream = new DataOutputStream(socket.getOutputStream());
                    writeOutputStream.write(msgToSend.getBytes());
                    writeOutputStream.flush();///check this
                    socket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                int index = Arrays.asList(REMOTE_PORT).indexOf(msgs[0]);
                String[] preferenceList = new String[]{REMOTE_PORT[(index + 1) % 5], REMOTE_PORT[(index + 2) % 5]};
                for (String replicaPort : preferenceList) {
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(replicaPort));
                        writeOutputStream = new DataOutputStream(socket.getOutputStream());
                        writeOutputStream.write(msgToSend.getBytes());
                        writeOutputStream.flush();
                        socket.close();
                    } catch (Exception e) {
                        Log.e(TAG,e.getMessage());
                    }
                }
                return null;
            }
            else{
                try {
                    String msgToSend = "ReturnStarQuery" + ":" + msgs[1] + recordOfData.get(1)+":";
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0]));
                    writeOutputStream = new DataOutputStream(socket.getOutputStream());
                    writeOutputStream.write(msgToSend.getBytes());
                    writeOutputStream.flush();
                    socket.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    private class starQueryClientTask extends AsyncTask<String,Void,Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            Socket socket;
            DataOutputStream writeOutputStream ;
            if(msgs.length == 3){
                try {
                    String msgToSend = "ReturnQueryTask" + ":" + msgs[1] + ":" + msgs[2] + ":" + recordOfData.get(1) + ":";
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0]));
                    writeOutputStream = new DataOutputStream(socket.getOutputStream());
                    writeOutputStream.write(msgToSend.getBytes());
                    writeOutputStream.flush();
                    socket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else{
                try {
                    String msgToSend = "StarQuery" + ":" + msgs[0] + ":";
                    Integer portId = 0;
                    if(msgs[0].compareToIgnoreCase("5554")==0){
                        portId = 11108;
                    }else if(msgs[0].compareToIgnoreCase("5556")==0){
                        portId = 11112;
                    }else if(msgs[0].compareToIgnoreCase("5558")==0){
                        portId = 11116;
                    }else if(msgs[0].compareToIgnoreCase("5560")==0){
                        portId = 11120;
                    }
                    else if(msgs[0].compareToIgnoreCase("5562")==0){
                        portId = 11124;
                    }

                    for(String port :REMOTE_PORT) {
                        if (port != portId.toString()) {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                            writeOutputStream = new DataOutputStream(socket.getOutputStream());
                            writeOutputStream.write(msgToSend.getBytes());
                            writeOutputStream.flush();
                            socket.close();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return  null;
        }
    }

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        MatrixCursor msgCursor = new MatrixCursor(new String[]{"key","value"});
        String msgValue="" ;
        BufferedInputStream bis;
        int temp;
        if(selection.compareToIgnoreCase("@")==0){
            String[] files=getContext().fileList();
            for(String fileName: files){
                msgValue="";
                try {
                    bis = new BufferedInputStream(getContext().openFileInput(fileName));
                    while ((temp = bis.read()) != -1) {
                        msgValue += (char) temp;
                    }
                    bis.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                msgCursor.addRow(new String[]{fileName, msgValue});
            }
            return msgCursor;
        }else if(selection.compareToIgnoreCase("*")==0) {
            String[] files = getContext().fileList();
            //Reading from local avd
            for (String fileName : files) {
                msgValue = "";
                try {
                    bis = new BufferedInputStream(getContext().openFileInput(fileName));
                    while ((temp = bis.read()) != -1) {
                        msgValue += (char) temp;
                    }
                    bis.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                cursorWithStarMessages.addRow(new String[]{fileName, msgValue});
            }

                new starQueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recordOfData.get(0));
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {
                }
            return cursorWithStarMessages;
        }else{
            try {
                //        portNumber:NodeId:hashedNode:nextNode:nextToNextNode
                //         0            1      2         3         4

                queryResults.put(selection,"empty");
                String destNode = null;
                try {
                    String hashNewkey = genHash(selection);
                    if(genHash(NODES[0]).compareTo(hashNewkey) >=0 || genHash(NODES[4]).compareTo(hashNewkey)<0){
                        destNode = NODES[0];
                    }else if(genHash(NODES[1]).compareTo(hashNewkey)>=0 && genHash(NODES[0]).compareTo(hashNewkey)<0 ){
                        destNode = NODES[1];
                    }else if(genHash(NODES[2]).compareTo(hashNewkey)>=0 && genHash(NODES[1]).compareTo(hashNewkey)<0 ){
                        destNode = NODES[2];
                    }else if(genHash(NODES[3]).compareTo(hashNewkey)>=0 && genHash(NODES[2]).compareTo(hashNewkey)<0 ){
                        destNode = NODES[3];
                    }else if(genHash(NODES[4]).compareTo(hashNewkey)>=0 && genHash(NODES[3]).compareTo(hashNewkey)<0 ){
                        destNode = NODES[4];
                    }
                }catch(Exception e){
                    Log.e(TAG,e.getMessage());
                }
                if(destNode.equals(recordOfData.get(1))){
                    bis = new BufferedInputStream(getContext().openFileInput(selection));
                    while ((temp = bis.read()) != -1) {
                        msgValue += (char) temp;
                    }
                    bis.close();
                    msgCursor.addRow(new String[]{selection, msgValue});
                }else {
                    String originatingPort = recordOfData.get(0);

                    Integer portId = 0;
                    if(destNode.compareToIgnoreCase("5554")==0){
                        portId = 11108;
                    }else if(destNode.compareToIgnoreCase("5556")==0){
                        portId = 11112;
                    }else if(destNode.compareToIgnoreCase("5558")==0){
                        portId = 11116;
                    }else if(destNode.compareToIgnoreCase("5560")==0){
                        portId = 11120;
                    }
                    else if (destNode.compareToIgnoreCase("5562")==0){
                        portId = 11124;
                    }

                    new queryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portId.toString(), selection, originatingPort);
                    while(true) {
                        if (queryResults.get(selection) != null && !queryResults.get(selection).equals("empty")) {
                            break;
                        }
                    }
                    if (queryResults.get(selection) != null && !queryResults.get(selection).equals("empty")) {
                        msgCursor.addRow(new String[]{selection, queryResults.get(selection)});
                        return msgCursor;
                    }
                }
            }catch(Exception e){
                Log.e(TAG, e.getMessage());
            }
        }
        return msgCursor;

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


    protected  void callUpdateClientTask(String partOfMessage,String rowList){
        new insertReplicateUpdate().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, partOfMessage, rowList);
    }

    private class insertReplicateUpdate extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            if(msgs.length == 2){
                Integer portId = 0;
                if (msgs[0].compareToIgnoreCase("5554") == 0) {
                    portId = 11108;
                } else if (msgs[0].compareToIgnoreCase("5556") == 0) {
                    portId = 11112;
                } else if (msgs[0].compareToIgnoreCase("5558") == 0) {
                    portId = 11116;
                } else if (msgs[0].compareToIgnoreCase("5560") == 0) {
                    portId = 11120;
                } else if (msgs[0].compareToIgnoreCase("5562") == 0) {
                    portId = 11124;
                }
                try {
                    String msgToSend = "UpdateTask" + ":" + msgs[1];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portId);
                    PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                    pw.println(msgToSend);
                    pw.flush();
                    socket.close();
                } catch (Exception e) {
                    Log.e(TAG, e.getMessage());
                }
            }else{
                if(msgs[3].compareToIgnoreCase("insert")==0){
                    Socket socket;
                    DataOutputStream writeOutputStream;
                    try {

                        String msgToSend = "InsertTask"+":"+msgs[1]+":" + msgs[2] + ":";
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0]));
                        writeOutputStream = new DataOutputStream(socket.getOutputStream());
                        writeOutputStream.write(msgToSend.getBytes());
                        writeOutputStream.flush();
                        socket.close();
                    } catch (Exception e) {
                        Log.e(TAG, e.getMessage());
                    }
                }else{
                    int index= Arrays.asList(REMOTE_PORT).indexOf(msgs[0]);
                    //Find the next 2 nodes on which to replicate
                    String[] preferenceList = new String[]{REMOTE_PORT[(index+1)%5],REMOTE_PORT[(index+2)%5]};
                    Log.v(TAG,"preferenceList:"+preferenceList[0]+":"+preferenceList[1]);
                    for(String replicatePort: preferenceList){
                        try {
                            //Try replicating the data on two other nodes
                            String msgToSend = "InsertReplicateTask" + ":" + msgs[1] + ":" + msgs[2] + ":";
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(replicatePort));
                            DataOutputStream writeOutputStream = new DataOutputStream(socket.getOutputStream());
                            writeOutputStream.write(msgToSend.getBytes());
                            writeOutputStream.flush();
                            socket.close();
                        } catch (Exception e) {
                            Log.e(TAG, e.getMessage());
                        }
                    }
                }
            }
            return null;
        }
    }
}
