package com.talis.zfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jvnet.solaris.libzfs.LibZFS;
import org.jvnet.solaris.libzfs.ZFSFileSystem;
import org.jvnet.solaris.libzfs.ZFSSnapshot;
import org.jvnet.solaris.libzfs.ZFSType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

	private final static Logger LOG = LoggerFactory.getLogger(Main.class);	
	private final Lock lock;
	private final AtomicInteger sequence;
	private final LibZFS zfs;
	private ZFSFileSystem fileSystem; 
	private File mountpoint;
	private File datafile;
		
	public Main(){
		lock = new ReentrantLock();
		sequence = new AtomicInteger(0);
		 zfs = new LibZFS();
				
		LOG.info("Creating zfs filesystem named \"beobal\" in pool \"testpool\"");
		fileSystem = (ZFSFileSystem)zfs.create("testpool/beobal", ZFSType.FILESYSTEM, null);
		LOG.info("Created zfs filesystem");
				
		mountpoint = new File("/var/data/beobal");
		mountpoint.mkdirs();
		
		LOG.info("Mounting filesystem at " + mountpoint.getAbsolutePath());
		fileSystem.setMountPoint(mountpoint);
		fileSystem.mount();
		LOG.info("Mounted zfs filesystem");
		
		datafile = new File(mountpoint, "data.txt");
		if (datafile.exists()){
			datafile.delete();
		}
		try {
			datafile.createNewFile();
		} catch (IOException e) {
			LOG.error("Couldn't create datafile", e);
			System.exit(1);
		}
		LOG.info("Created datafile, waiting for actions to process");
	}
	
	public void doSnapshot() {
		LOG.info("Taking snapshot, obtaining lock");
		lock.lock();
		LOG.info("Obtained lock");
		try{
			int seq = sequence.get() - 1;
			LOG.info("Creating zfs snapshot. Sequence is " + seq);
			fileSystem.createSnapshot("" + seq);
			LOG.info("Created zfs snapshot");
		}finally{
			LOG.info("Releasing lock");
			lock.unlock();
			LOG.info("Lock released");
		}	
	}

	public void doUpdate() {
		LOG.info("Writing update, obtaining lock");
		lock.lock();
		LOG.info("Obtained lock");
		try {
			int seq = sequence.getAndIncrement();
			LOG.info("Doing update with sequence " + seq);
			BufferedWriter writer = new BufferedWriter(new FileWriter(datafile, true));
			writer.write("" + seq);
			writer.write("\n");
			writer.flush();
			writer.close();
		} catch (IOException e) {
			LOG.error("Error when writing update to datafile", e);
			e.printStackTrace();
		}finally{
			LOG.info("Releasing lock");
			lock.unlock();
			LOG.info("Lock released");
		}
	}
	
	public void listSnapshots(){
		for (ZFSSnapshot snapshot : fileSystem.snapshots()){
			LOG.info("SNAPSHOT: " + snapshot.getName());
			System.out.println(snapshot.getName());
		}
	}
	
	
	public static void main(String[] args) throws IOException{
		LOG.info("Starting test");
		Main main = new Main();
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		while (true){
			System.out.print("Enter u to update, s to snapshot, l to list snapshots, or any other key to exit: ");
			String input = reader.readLine();
			if (input.equals("u")){
				main.doUpdate();		  
			}else if( input.equals("s")){
				main.doSnapshot();
			}else if( input.equals("l")){
				main.listSnapshots();
			}else{
				System.out.println("Exiting");
				System.exit(0);
			}
		}
	}
	
}
