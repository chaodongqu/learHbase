package org.springframework.samples.hadoop.hbase;

import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Repository;

@Repository
public class UserRepository {

	@Autowired
	private HbaseTemplate hbaseTemplate;

	private String tableName = "users";

	public static byte[] CF_INFO = Bytes.toBytes("cfInfo");

	private byte[] qUser = Bytes.toBytes("user");
	private byte[] qEmail = Bytes.toBytes("email");
	private byte[] qPassword = Bytes.toBytes("password");

	public List<User> findAll() {
		return hbaseTemplate.find(tableName, "cfInfo", new RowMapper<User>() {
			@Override
			public User mapRow(Result result, int rowNum) throws Exception {
				return new User(Bytes.toString(result.getValue(CF_INFO, qUser)), 
							    Bytes.toString(result.getValue(CF_INFO, qEmail)),
							    Bytes.toString(result.getValue(CF_INFO, qPassword)));
			}
		});

	}

	// 获取row为 user2的数据
	public  User findOne(){
		return
				hbaseTemplate.get(tableName ,"user2" , new RowMapper<User>() {
			@Override
			public User mapRow(Result result, int rowNum) throws Exception {
				return new User(Bytes.toString(result.getValue(CF_INFO, qUser)),
						Bytes.toString(result.getValue(CF_INFO, qEmail)),
						Bytes.toString(result.getValue(CF_INFO, qPassword)));
			}
		});
		// return null ;
	}

	// 获取 user1 -3 ,scan 方法测试
	public  List<User>  findSome()
	{

		byte[] startRow = Bytes.toBytes("user1");
		byte[] endRow = Bytes.toBytes("user4");
		Scan s = new Scan( startRow,endRow);

		return hbaseTemplate.find(tableName,s, new RowMapper<User>() {
			@Override
			public User mapRow(Result result, int rowNum) throws Exception {
				return new User(Bytes.toString(result.getValue(CF_INFO, qUser)),
						Bytes.toString(result.getValue(CF_INFO, qEmail)),
						Bytes.toString(result.getValue(CF_INFO, qPassword)));
			}
		});

	}


	public User save(final String userName, final String email,
			final String password) {
		return hbaseTemplate.execute(tableName, new TableCallback<User>() {
			public User doInTable(HTableInterface table) throws Throwable {
				User user = new User(userName, email, password);
				Put p = new Put(Bytes.toBytes(user.getName()));
				p.add(CF_INFO, qUser, Bytes.toBytes(user.getName()));
				p.add(CF_INFO, qEmail, Bytes.toBytes(user.getEmail()));
				p.add(CF_INFO, qPassword, Bytes.toBytes(user.getPassword()));
				table.put(p);
				return user;
				
			}
		});
	}

}
