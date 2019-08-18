package db.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import db.DBConnection;
import entity.Item;
import entity.Item.ItemBuilder;
import external.TicketMasterAPI;

public class MySQLConnection implements DBConnection {

	private Connection conn;
	
	public MySQLConnection() {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver").getConstructor().newInstance();
			conn = DriverManager.getConnection(MySQLDBUtil.URL);		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void close() {
		if (conn == null) {
			System.out.println("DB connection is null");
			return;
		}
		try {
			conn.close();
		} catch(Exception e) {
			e.printStackTrace();
		} 
	}

	@Override
	public void setFavoriteItems(String userId, List<String> itemIds) {
		if (conn == null) {
			System.out.println("DB connection is null");
			return;			
		}
		try {
			String sql = "INSERT IGNORE INTO history(user_id, item_id) VALUES (?, ?)";
			PreparedStatement pStatement = conn.prepareStatement(sql);
			pStatement.setString(1, userId);
			for (String itemId : itemIds) {
				pStatement.setString(2, itemId);
				pStatement.execute();	
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void unsetFavoriteItems(String userId, List<String> itemIds) {
		if (conn == null) {
			System.out.println("DB connection is null");
			return;			
		}
		try {
			String sql = "DELETE FROM history WHERE user_id = ? AND item_id = ?";
			PreparedStatement pStatement = conn.prepareStatement(sql);
			pStatement.setString(1, userId);
			for (String itemId : itemIds) {
				pStatement.setString(2, itemId);
				pStatement.execute();	
			}
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

	@Override
	public Set<String> getFavoriteItemIds(String userId) {
		if (conn == null) {
			System.out.println("DB Connection is null");
			return new HashSet<>();
		}
		Set<String> favoriteItems = new HashSet<>();
		try {
			String sql = "SELECT * FROM history WHERE user_id = ?";
			PreparedStatement pStatement = conn.prepareStatement(sql);
			pStatement.setString(1, userId);
			
			ResultSet rSet = pStatement.executeQuery();
			while (rSet.next()) {
				String itemId = rSet.getString("item_id");
				favoriteItems.add(itemId);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return favoriteItems;
	}

	@Override
	public Set<Item> getFavoriteItems(String userId) {
		if (conn == null) {
			return new HashSet<>();
		}
		
		Set<Item> favoriteItems = new HashSet<>();
		Set<String> itemIds = getFavoriteItemIds(userId);
		
		try {
			String sql = "SELECT * FROM items WHERE item_id = ?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			for (String itemId : itemIds) {
				stmt.setString(1, itemId);
				
				ResultSet rs = stmt.executeQuery();
				
				ItemBuilder builder = new ItemBuilder();
				
				while (rs.next()) {
					builder.setItemId(rs.getString("item_id"));
					builder.setName(rs.getString("name"));
					builder.setAddress(rs.getString("address"));
					builder.setImageUrl(rs.getString("image_url"));
					builder.setUrl(rs.getString("url"));
					builder.setCategories(getCategories(itemId));
					builder.setDistance(rs.getDouble("distance"));
					builder.setRating(rs.getDouble("rating"));
					
					favoriteItems.add(builder.build());
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return favoriteItems;
	}

	@Override
	public Set<String> getCategories(String itemId) {
		if (conn == null) {
			return null;
		}
		Set<String> categories = new HashSet<>();
		try {
			String sql = "SELECT category from categories WHERE item_id = ? ";
			PreparedStatement statement = conn.prepareStatement(sql);
			statement.setString(1, itemId);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				String category = rs.getString("category");
				categories.add(category);
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return categories;
	}

	@Override
	public List<Item> searchItems(double lat, double lon, String term) {
		TicketMasterAPI api = new TicketMasterAPI();
		List<Item> items = api.search(lat, lon, term);
		for (Item item : items) {
			saveItem(item);
		}
		return items;
	}

	@Override
	public void saveItem(Item item) {
		if (conn == null) {
			System.err.println("DB connection failed");
			return;
		}
		try {
			String sql = "INSERT IGNORE INTO items(item_id, name, rating, address, image_url, url, distance) VALUES(?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement pStatement = conn.prepareStatement(sql);
//			System.out.println(item.getItemId());
			pStatement.setString(1, item.getItemId());
			pStatement.setString(2, item.getName());
			pStatement.setDouble(3, item.getRating());
			pStatement.setString(4, item.getAddress());
			pStatement.setString(5, item.getImageUrl());
			pStatement.setString(6, item.getUrl());
			pStatement.setDouble(7, item.getDistance());
			pStatement.execute();

			sql = "INSERT IGNORE INTO categories VALUES(?, ?)";
			pStatement = conn.prepareStatement(sql);
			pStatement.setString(1, item.getItemId());
			for (String category: item.getCategories()) {
				pStatement.setString(2, category);
				pStatement.execute();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getFullname(String userId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean verifyLogin(String userId, String password) {
		// TODO Auto-generated method stub
		return false;
	}

}
