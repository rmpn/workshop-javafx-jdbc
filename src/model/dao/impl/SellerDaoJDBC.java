package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;


public class SellerDaoJDBC implements SellerDao {

	private Connection conn;

	public SellerDaoJDBC(Connection conn) {

		this.conn = conn;

	}

	@Override
	public void insert(Seller obj) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = "INSERT INTO coursejdbc.seller " + "(Name, Email, BirthDate, BaseSalary, DepartmentId) "
					+ "VALUES(?,?,?,?,?) ";
			ps = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			ps.setString(1, obj.getName());
			ps.setString(2, obj.getEmail());
			ps.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			ps.setDouble(4, obj.getBaseSalary());
			ps.setInt(5, obj.getDepartment().getId());

			int rowsAffected = ps.executeUpdate();
			if (rowsAffected > 0) {

				rs = ps.getGeneratedKeys();
				if (rs.next()) {

					obj.setId(rs.getInt(1));
				}

			} else {

				throw new DbException("Unexpected error! No rows affected!");
			}

		} catch (SQLException e) {

			throw new DbException(e.getMessage());

		} finally {

			DB.closeResultSet(rs);
			DB.closeStatement(ps);

		}

	}

	@Override
	public void update(Seller obj) {

		
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = "UPDATE coursejdbc.seller "
				    +"SET Name=?, Email=?, BirthDate=?, BaseSalary=?, DepartmentId=? "
					+"WHERE Id=? ";
		    ps = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			ps.setString(1, obj.getName());
			ps.setString(2, obj.getEmail());
			ps.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			ps.setDouble(4, obj.getBaseSalary());
			ps.setInt(5, obj.getDepartment().getId());
			ps.setInt(6, obj.getId());

			int rowsAffected = ps.executeUpdate();
			if (rowsAffected > 0) {

				rs = ps.getGeneratedKeys();
				if (rs.next()) {

					obj.setId(rs.getInt(1));
				}

			} else {

				throw new DbException("Unexpected error! No rows affected!");
			}

		} catch (SQLException e) {

			throw new DbException(e.getMessage());

		} finally {

			DB.closeResultSet(rs);
			DB.closeStatement(ps);

		}

		
		
	}

	@Override
	public void deleteById(Integer id) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			String sql = "DELETE FROM coursejdbc.seller WHERE Id=? ";
		    ps = conn.prepareStatement(sql,PreparedStatement.RETURN_GENERATED_KEYS);
			ps.setInt(1, id);

			int rowsAffected = ps.executeUpdate();
			System.out.println(rowsAffected);
			if (rowsAffected > 0) {

				rs = ps.getGeneratedKeys();
				System.out.println("Registro deletado!!");

			} else {

				throw new DbException("Unexpected error! No rows affected!");
			}

		} catch (SQLException e) {

			throw new DbException(e.getMessage());

		} finally {

			DB.closeResultSet(rs);
			DB.closeStatement(ps);

		}

		
	}

	@Override
	public Seller findById(Integer id) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = "SELECT seller.*, department.Name as DepName " + "" + "FROM seller "
					+ "INNER JOIN department on (department.Id = seller.Id ) " + "WHERE seller.Id = ?";

			ps = conn.prepareStatement(sql);
			ps.setInt(1, id);

			rs = ps.executeQuery();

			if (rs.next()) {

				Department dep = instantiateDepartment(rs);
				Seller obj = instantiateSeller(rs, dep);

				return obj;

			} else {

				return null;
			}

		} catch (SQLException e) {

			throw new DbException(e.getMessage());

		} finally {

			DB.closeResultSet(rs);
			DB.closeStatement(ps);

		}

	}

	private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {
		Seller obj = new Seller();
		obj.setId(rs.getInt("Id"));
		obj.setName(rs.getString("Name"));
		obj.setEmail(rs.getString("Email"));
		obj.setBaseSalary(rs.getDouble("BaseSalary"));
		obj.setBirthDate(rs.getDate("BirthDate"));
		obj.setDepartment(dep);
		return obj;
	}

	private Department instantiateDepartment(ResultSet rs) throws SQLException {

		Department dep = new Department();

		dep.setId(rs.getInt("DepartmentId"));
		dep.setName(rs.getString("DepName"));
		return dep;

	}

	@Override
	public List<Seller> findAll() {

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = "SELECT seller.*, department.Name as DepName " + "FROM seller "
					+ "INNER JOIN department on (department.Id = seller.Id ) " + "ORDER BY seller.Name ";

			ps = conn.prepareStatement(sql);

			rs = ps.executeQuery();

			List<Seller> list = new ArrayList<>();
			Map<Integer, Department> map = new HashMap<>();

			while (rs.next()) {

				Department dep = map.get(rs.getInt("DepartmentId"));
				if (dep == null) {

					dep = instantiateDepartment(rs);
					map.put(rs.getInt("DepartmentId"), dep);

				}

				Seller obj = instantiateSeller(rs, dep);
				list.add(obj);

			}

			return list;

		} catch (SQLException e) {

			throw new DbException(e.getMessage());

		} finally {

			DB.closeResultSet(rs);
			DB.closeStatement(ps);

		}
	}

	@Override
	public List<Seller> findByDepartment(Department department) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = "SELECT seller.*, department.Name as DepName " + "FROM seller "
					+ "INNER JOIN department on (department.Id = seller.Id ) " + "WHERE DepartmentId = ? "
					+ "ORDER BY seller.Name ";

			ps = conn.prepareStatement(sql);
			ps.setInt(1, department.getId());

			rs = ps.executeQuery();

			List<Seller> list = new ArrayList<>();
			Map<Integer, Department> map = new HashMap<>();

			while (rs.next()) {

				Department dep = map.get(rs.getInt("DepartmentId"));
				if (dep == null) {

					dep = instantiateDepartment(rs);
					map.put(rs.getInt("DepartmentId"), dep);

				}

				Seller obj = instantiateSeller(rs, dep);
				list.add(obj);

			}

			return list;

		} catch (SQLException e) {

			throw new DbException(e.getMessage());

		} finally {

			DB.closeResultSet(rs);
			DB.closeStatement(ps);

		}
	}

}
