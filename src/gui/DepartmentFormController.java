package gui;

import java.net.URL;
import java.util.ResourceBundle;

import gui.util.Constraints;
import javafx.event.Event;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;

public class DepartmentFormController implements Initializable {

	@FXML
	private TextField txtId;

	@FXML
	private TextField txtName;

	@FXML
	private Label labelErrorName;

	@FXML
	private Button btSave;

	@FXML
	private Button brCancel;

	@FXML
	public void onBtSaveAction(Event event) {
		System.out.println("onBtSaveAction(Event event)");
	}

	@FXML
	public void onBtCancelAction() {
		System.out.println("onBtSaveAction(Event event)");
	}

	@Override
	public void initialize(URL url, ResourceBundle rb) {

		initializeNodes();

	}

	private void initializeNodes() {

		Constraints.setTextFieldDouble(txtId);
		Constraints.setTextFieldMaxLength(txtName, 30);
	}

}
