package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao {

	private Connection conn;

	public DepartmentDaoJDBC(Connection conn) {

		this.conn = conn;

	}

	@Override
	public void insert(Department obj) {

		PreparedStatement ps = null;
		ResultSet rs = null;

		String sql = "INSERT INTO coursejdbc.department (Name) VALUES (?) ";

		try {

			ps = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			ps.setString(1, obj.getName());

			int rowsAffected = ps.executeUpdate();
			if (rowsAffected > 0) {
				rs = ps.getResultSet();
				System.out.println("Registro inserido com sucesso!");
			} else {
				System.out.println("Não houve inserção.");
			}

		} catch (SQLException e) {

			throw new DbException(e.getMessage());

		} finally {

			DB.closeResultSet(rs);
			DB.closeStatement(ps);

		}

	}

	@Override
	public void update(Department obj) {
		PreparedStatement ps = null;
		ResultSet rs = null;

		String sql = "UPDATE coursejdbc.department "
		             +"SET Name = ? "
		             +"WHERE Id = ? ";

		try {

			ps = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			ps.setString(1, obj.getName());
			ps.setInt(2, obj.getId());

			int rowsAffected = ps.executeUpdate();
			if (rowsAffected > 0) {
				rs = ps.getResultSet();
				System.out.println("Registro alterado com sucesso!");
			} else {
				System.out.println("Não houve alteração no BD.");
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

		String sql = "DELETE FROM coursejdbc.department " + "WHERE Id = ? ";

		try {

			ps = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			ps.setInt(1, id);

			int rowsAffected = ps.executeUpdate();
			if (rowsAffected > 0) {
				rs = ps.getResultSet();
				System.out.println("Registro excluído com sucesso!");
			} else {
				System.out.println("Não houve exclusão no BD.");
			}

		} catch (SQLException e) {

			throw new DbException(e.getMessage());
		} finally {

			DB.closeResultSet(rs);
			DB.closeStatement(ps);

		}

	}

	@Override
	public Department findById(Integer id) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		Department department = null;
		String sql = "SELECT Id, Name FROM department " + "WHERE Id = ? ";

		try {

			ps = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			ps.setInt(1, id);
			//
			rs = ps.executeQuery();
			if (rs.next()) {
				department = new Department(rs.getInt("Id"), rs.getString("Name"));

			}
			return department;

		} catch (SQLException e) {

			throw new DbException(e.getMessage());
		} finally {

			DB.closeResultSet(rs);
			DB.closeStatement(ps);

		}

	}

	@Override
	public List<Department> findAll() {

		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Department> list = new ArrayList<>();
		String sql = "SELECT Id, Name FROM department ORDER BY Name";

		try {

			ps = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			//
			rs = ps.executeQuery();
			while (rs.next()) {

				list.add(new Department(rs.getInt("Id"), rs.getString("Name")));

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
