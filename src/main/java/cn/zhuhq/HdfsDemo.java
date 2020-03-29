package cn.zhuhq;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsDemo {
    
//	连接hadoop集群，获取集群的文件系统
	public FileSystem getClient() throws IOException, InterruptedException, URISyntaxException {
		// TODO Auto-generated constructor stub
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.102:9000"), conf , "hadoop");
		
		System.out.println("Connect to hadoop cluster successfully!");
		return fs;
	}
	
//	创建目录
	public void makeDir() throws IOException, InterruptedException, URISyntaxException {
		FileSystem client = this.getClient();
		Path path = new Path("/zhuhq/xxx");
		boolean mkdirs = client.mkdirs(path);
		if (mkdirs) {
			System.out.println("You have made the dir "+path+" succesfully!");
		}else {
			System.out.println("You have made the dir "+path+" failed!");
		}
			client.close();
			
		
	}
//	删除目录	
	public void DelDir() throws IOException, InterruptedException, URISyntaxException {
		FileSystem client = this.getClient();
		Path path = new Path("/zhuhq/testz");
		boolean delete = client.delete(path, true);
		if (delete) {
			System.out.println("You have deleted the dir "+path+" succesfully!" );
		} else {
			System.out.println("You have deleted the dir "+path+" failed!" );
		}
	}
	
//	上传文件
	public void uploadFile() throws IOException, InterruptedException, URISyntaxException {
		FileSystem client = this.getClient();
		Path dst = new Path("/zhuhq/test/");
		Path src = new Path("e:/study/hello.txt");
		client.copyFromLocalFile(src, dst);
		System.out.println("the file "+src+" is uploading to "+dst+" succesfuly!");
		client.close();
	}
	
// 下载文件
	public void downloadFile() throws IOException, InterruptedException, URISyntaxException {
		FileSystem client = this.getClient();
		
		Path dst = new Path("e:/study");
		Path src = new Path("/usr/hadoop/input/wc.input");
		client.copyToLocalFile(src, dst);
		System.out.println("the file "+src+" is downloading to "+dst+" succesfuly!");
		client.close();
	}
//	文件名更改
	public void renameFile() throws IOException, InterruptedException, URISyntaxException {
		FileSystem client = this.getClient();
		
		Path dst = new Path("/zhuhq/test/test.txt");
		Path src = new Path("/zhuhq/test/hello.txt");
		boolean rename = client.rename(src, dst);
		if (rename) {
			System.out.println("the file "+src+" is renamed to "+dst+" succesfully!");
		} else {
			System.out.println("the file "+src+" is renamed to "+dst+" failed!");
		}
		client.close();
	}
//	获取hdfs文件信息
	@Test
	public void getListFileInfo() throws IOException, InterruptedException, URISyntaxException {
		FileSystem client = this.getClient();
		
		RemoteIterator<LocatedFileStatus> listFiles = client.listFiles(new Path("/"), true);
		
		while (listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();
			
//			输出文件详情
//			文件名称
			System.out.println(status.getPath().getName());
//			长度
			System.out.println(status.getLen());
//			权限
			System.out.println(status.getPermission());
//			分组
			System.out.println(status.getGroup());
			
//			获取存储块信息
			
			BlockLocation[] locations = status.getBlockLocations();
			for (BlockLocation blockLocation : locations) {
//				获取块存储的主机节点
				String[] hosts = blockLocation.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("-------------------班长的分割线-----------------------------------");
		}
		client.close();
	}
}
