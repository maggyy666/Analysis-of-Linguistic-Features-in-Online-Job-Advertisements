"""
app.py

PyQt5 GUI application for job posting analysis pipeline.

Features:
- Main view with sidebar for data processing and model training
- Try it out view for interactive predictions
- File upload for datasets
- Integration with data processing and model training scripts
"""

import sys
import subprocess
import os
from pathlib import Path
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QTextEdit, QPushButton, QMessageBox, QFileDialog,
    QStackedWidget, QListWidget, QListWidgetItem, QFormLayout,
    QLineEdit, QGroupBox, QScrollArea, QFrame, QSplitter
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QIcon

MODEL_PATH = Path("model_output/baseline_tfidf_linearsvc.joblib")


class ProcessThread(QThread):
    """Thread for running external scripts without freezing UI."""
    finished = pyqtSignal(str, bool)  # message, success
    
    def __init__(self, script_path, args=None):
        super().__init__()
        self.script_path = script_path
        self.args = args or []
    
    def run(self):
        try:
            cmd = [sys.executable, str(self.script_path)] + self.args
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=Path.cwd()
            )
            if result.returncode == 0:
                self.finished.emit(result.stdout, True)
            else:
                self.finished.emit(result.stderr or result.stdout, False)
        except Exception as e:
            self.finished.emit(str(e), False)


class MainView(QWidget):
    """Main view with sidebar for data processing and model training."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()
    
    def init_ui(self):
        layout = QHBoxLayout()
        self.setLayout(layout)
        
        # Main content area
        content_area = QScrollArea()
        content_widget = QWidget()
        content_layout = QVBoxLayout()
        content_widget.setLayout(content_layout)
        
        # Title
        title = QLabel("Data Processing & Model Training")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title.setFont(title_font)
        title.setAlignment(Qt.AlignCenter)
        content_layout.addWidget(title)
        
        # Content stack - create first
        self.content_stack = QStackedWidget()
        
        # Input Dataset view
        input_view = self.create_input_dataset_view()
        self.content_stack.addWidget(input_view)
        
        # Data Processing view
        processing_view = self.create_data_processing_view()
        self.content_stack.addWidget(processing_view)
        
        # Model Training view
        model_view = self.create_model_training_view()
        self.content_stack.addWidget(model_view)
        
        content_layout.addWidget(self.content_stack)
        content_layout.addStretch()
        
        content_area.setWidget(content_widget)
        content_area.setWidgetResizable(True)
        
        # Sidebar - create after content_stack
        sidebar = self.create_sidebar()
        sidebar.setMaximumWidth(300)
        
        # Connect sidebar to content_stack
        self.category_list.currentRowChanged.connect(self.switch_content)
        self.category_list.setCurrentRow(0)
        
        layout.addWidget(sidebar)
        layout.addWidget(content_area, stretch=1)
    
    def create_sidebar(self):
        sidebar = QFrame()
        sidebar.setFrameStyle(QFrame.StyledPanel)
        sidebar.setStyleSheet("background-color: #f5f5f5;")
        sidebar_layout = QVBoxLayout()
        sidebar.setLayout(sidebar_layout)
        
        # Sidebar title
        sidebar_title = QLabel("Navigation")
        sidebar_title.setFont(QFont("Arial", 12, QFont.Bold))
        sidebar_layout.addWidget(sidebar_title)
        
        # Category list
        self.category_list = QListWidget()
        self.category_list.setStyleSheet("""
            QListWidget::item {
                padding: 10px;
                border-bottom: 1px solid #ddd;
            }
            QListWidget::item:selected {
                background-color: #2196F3;
                color: white;
            }
        """)
        
        categories = [
            "Input Dataset",
            "Data Processing",
            "Model Training"
        ]
        
        for cat in categories:
            item = QListWidgetItem(cat)
            self.category_list.addItem(item)
        
        sidebar_layout.addWidget(self.category_list)
        
        return sidebar
    
    def switch_content(self, index):
        """Switch content view based on sidebar selection."""
        self.content_stack.setCurrentIndex(index)
    
    def create_input_dataset_view(self):
        """Create input dataset upload view."""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        group = QGroupBox("Upload Dataset")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        form_layout = QFormLayout()
        group.setLayout(form_layout)
        
        # File path display
        self.dataset_path_label = QLabel("No file selected")
        self.dataset_path_label.setStyleSheet("color: #666; padding: 5px;")
        
        # Browse button
        browse_btn = QPushButton("Browse CSV File...")
        browse_btn.setStyleSheet(
            "QPushButton { background-color: #2196F3; color: white; "
            "font-weight: bold; padding: 8px; border-radius: 4px; }"
            "QPushButton:hover { background-color: #0b7dda; }"
        )
        browse_btn.clicked.connect(self.browse_dataset)
        
        path_layout = QHBoxLayout()
        path_layout.addWidget(self.dataset_path_label, stretch=1)
        path_layout.addWidget(browse_btn)
        form_layout.addRow("Dataset:", path_layout)
        
        # Info
        info_label = QLabel(
            "Upload a CSV file containing job postings.\n"
            "Required columns: job_id, title, description, company_name, etc."
        )
        info_label.setWordWrap(True)
        info_label.setStyleSheet("color: #666; padding: 10px;")
        
        layout.addWidget(group)
        layout.addWidget(info_label)
        layout.addStretch()
        
        return widget
    
    def browse_dataset(self):
        """Open file dialog to select dataset."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Dataset CSV",
            str(Path.cwd()),
            "CSV Files (*.csv);;All Files (*)"
        )
        if file_path:
            self.dataset_path_label.setText(file_path)
            self.dataset_path_label.setStyleSheet("color: green; padding: 5px;")
    
    def create_data_processing_view(self):
        """Create data processing view."""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        group = QGroupBox("Clean Datasets")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        group_layout = QVBoxLayout()
        group.setLayout(group_layout)
        
        # PL button (disabled for now)
        pl_btn = QPushButton("Clean PL Dataset")
        pl_btn.setStyleSheet(
            "QPushButton { background-color: #cccccc; color: #666; "
            "font-weight: bold; padding: 10px; border-radius: 4px; }"
        )
        pl_btn.setEnabled(False)
        pl_btn.setToolTip("Not implemented yet")
        group_layout.addWidget(pl_btn)
        
        # EN button
        self.en_btn = QPushButton("Clean EN Dataset")
        self.en_btn.setStyleSheet(
            "QPushButton { background-color: #4CAF50; color: white; "
            "font-weight: bold; padding: 10px; border-radius: 4px; }"
            "QPushButton:hover { background-color: #45a049; }"
            "QPushButton:disabled { background-color: #cccccc; }"
        )
        self.en_btn.clicked.connect(self.clean_en_dataset)
        group_layout.addWidget(self.en_btn)
        
        # Status/output area
        self.processing_output = QTextEdit()
        self.processing_output.setReadOnly(True)
        self.processing_output.setMaximumHeight(200)
        self.processing_output.setPlaceholderText("Processing output will appear here...")
        group_layout.addWidget(QLabel("Output:"))
        group_layout.addWidget(self.processing_output)
        
        layout.addWidget(group)
        layout.addStretch()
        
        return widget
    
    def clean_en_dataset(self):
        """Run clean_en_dataset.py script."""
        script_path = Path("data_processing/clean_en_dataset.py")
        if not script_path.exists():
            QMessageBox.critical(
                self,
                "Script Not Found",
                f"Script not found at: {script_path}"
            )
            return
        
        self.en_btn.setEnabled(False)
        self.processing_output.clear()
        self.processing_output.append("Starting EN dataset cleaning...")
        
        self.process_thread = ProcessThread(script_path)
        self.process_thread.finished.connect(self.on_processing_finished)
        self.process_thread.start()
    
    def on_processing_finished(self, output, success):
        """Handle processing completion."""
        self.en_btn.setEnabled(True)
        self.processing_output.append(output)
        if success:
            self.processing_output.append("\n✓ Processing completed successfully!")
            QMessageBox.information(self, "Success", "EN dataset cleaning completed!")
        else:
            self.processing_output.append("\n✗ Processing failed!")
            QMessageBox.warning(self, "Error", f"Processing failed:\n{output[:500]}")
    
    def create_model_training_view(self):
        """Create model training view."""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        # Train Split section
        split_group = QGroupBox("Train Split")
        split_group.setFont(QFont("Arial", 12, QFont.Bold))
        split_layout = QVBoxLayout()
        split_group.setLayout(split_layout)
        
        split_info = QLabel(
            "Create train/validation/test splits from cleaned dataset.\n"
            "Output: en_train.csv, en_val.csv, en_test.csv"
        )
        split_info.setWordWrap(True)
        split_info.setStyleSheet("color: #666; padding: 5px;")
        split_layout.addWidget(split_info)
        
        self.split_btn = QPushButton("Create Train/Val/Test Split")
        self.split_btn.setStyleSheet(
            "QPushButton { background-color: #2196F3; color: white; "
            "font-weight: bold; padding: 10px; border-radius: 4px; }"
            "QPushButton:hover { background-color: #0b7dda; }"
            "QPushButton:disabled { background-color: #cccccc; }"
        )
        self.split_btn.clicked.connect(self.create_train_split)
        split_layout.addWidget(self.split_btn)
        
        self.split_output = QTextEdit()
        self.split_output.setReadOnly(True)
        self.split_output.setMaximumHeight(150)
        split_layout.addWidget(QLabel("Output:"))
        split_layout.addWidget(self.split_output)
        
        layout.addWidget(split_group)
        
        # Train Baseline section
        baseline_group = QGroupBox("Train Baseline Model")
        baseline_group.setFont(QFont("Arial", 12, QFont.Bold))
        baseline_layout = QVBoxLayout()
        baseline_group.setLayout(baseline_layout)
        
        baseline_info = QLabel(
            "Train TF-IDF + LinearSVC baseline model.\n"
            "Model will be saved to model_output/"
        )
        baseline_info.setWordWrap(True)
        baseline_info.setStyleSheet("color: #666; padding: 5px;")
        baseline_layout.addWidget(baseline_info)
        
        self.baseline_btn = QPushButton("Train Baseline Model")
        self.baseline_btn.setStyleSheet(
            "QPushButton { background-color: #9C27B0; color: white; "
            "font-weight: bold; padding: 10px; border-radius: 4px; }"
            "QPushButton:hover { background-color: #7b1fa2; }"
            "QPushButton:disabled { background-color: #cccccc; }"
        )
        self.baseline_btn.clicked.connect(self.train_baseline)
        baseline_layout.addWidget(self.baseline_btn)
        
        self.baseline_output = QTextEdit()
        self.baseline_output.setReadOnly(True)
        self.baseline_output.setMaximumHeight(150)
        baseline_layout.addWidget(QLabel("Output:"))
        baseline_layout.addWidget(self.baseline_output)
        
        layout.addWidget(baseline_group)
        layout.addStretch()
        
        return widget
    
    def create_train_split(self):
        """Run make_train_split.py script."""
        script_path = Path("model/make_train_split.py")
        if not script_path.exists():
            QMessageBox.critical(
                self,
                "Script Not Found",
                f"Script not found at: {script_path}"
            )
            return
        
        self.split_btn.setEnabled(False)
        self.split_output.clear()
        self.split_output.append("Creating train/val/test split...")
        
        self.split_thread = ProcessThread(script_path)
        self.split_thread.finished.connect(self.on_split_finished)
        self.split_thread.start()
    
    def on_split_finished(self, output, success):
        """Handle split completion."""
        self.split_btn.setEnabled(True)
        self.split_output.append(output)
        if success:
            self.split_output.append("\n✓ Split completed successfully!")
            QMessageBox.information(self, "Success", "Train/val/test split created!")
        else:
            self.split_output.append("\n✗ Split failed!")
            QMessageBox.warning(self, "Error", f"Split failed:\n{output[:500]}")
    
    def train_baseline(self):
        """Run train_baseline_tfidf.py script."""
        script_path = Path("model/train_baseline_tfidf.py")
        if not script_path.exists():
            QMessageBox.critical(
                self,
                "Script Not Found",
                f"Script not found at: {script_path}"
            )
            return
        
        self.baseline_btn.setEnabled(False)
        self.baseline_output.clear()
        self.baseline_output.append("Training baseline model...")
        
        self.baseline_thread = ProcessThread(script_path)
        self.baseline_thread.finished.connect(self.on_baseline_finished)
        self.baseline_thread.start()
    
    def on_baseline_finished(self, output, success):
        """Handle baseline training completion."""
        self.baseline_btn.setEnabled(True)
        self.baseline_output.append(output)
        if success:
            self.baseline_output.append("\n✓ Model training completed successfully!")
            QMessageBox.information(self, "Success", "Baseline model trained and saved!")
        else:
            self.baseline_output.append("\n✗ Training failed!")
            QMessageBox.warning(self, "Error", f"Training failed:\n{output[:500]}")


class TryItOutView(QWidget):
    """View for interactive predictions."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.model = None
        self.init_ui()
        self.load_model()
    
    def init_ui(self):
        layout = QVBoxLayout()
        self.setLayout(layout)
        
        # Title
        title = QLabel("Try It Out - Experience Level Predictor")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title.setFont(title_font)
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)
        
        # Model status
        self.status_label = QLabel("Model: Not loaded")
        self.status_label.setStyleSheet("color: red; font-weight: bold;")
        layout.addWidget(self.status_label)
        
        # Input fields
        title_input_label = QLabel("Job Title:")
        title_input_label.setFont(QFont("Arial", 10, QFont.Bold))
        layout.addWidget(title_input_label)
        
        self.title_input = QTextEdit()
        self.title_input.setPlaceholderText("Enter job title (e.g., 'Senior Software Engineer')")
        self.title_input.setMaximumHeight(60)
        layout.addWidget(self.title_input)
        
        desc_input_label = QLabel("Job Description:")
        desc_input_label.setFont(QFont("Arial", 10, QFont.Bold))
        layout.addWidget(desc_input_label)
        
        self.desc_input = QTextEdit()
        self.desc_input.setPlaceholderText("Enter job description...")
        layout.addWidget(self.desc_input)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        self.predict_btn = QPushButton("Predict Experience Level")
        self.predict_btn.setStyleSheet(
            "QPushButton { background-color: #4CAF50; color: white; "
            "font-weight: bold; padding: 10px; border-radius: 5px; }"
            "QPushButton:hover { background-color: #45a049; }"
            "QPushButton:disabled { background-color: #cccccc; }"
        )
        self.predict_btn.clicked.connect(self.predict)
        self.predict_btn.setEnabled(False)
        button_layout.addWidget(self.predict_btn)
        
        clear_btn = QPushButton("Clear")
        clear_btn.setStyleSheet(
            "QPushButton { background-color: #f44336; color: white; "
            "font-weight: bold; padding: 10px; border-radius: 5px; }"
            "QPushButton:hover { background-color: #da190b; }"
        )
        clear_btn.clicked.connect(self.clear_inputs)
        button_layout.addWidget(clear_btn)
        
        layout.addLayout(button_layout)
        
        # Result
        result_label = QLabel("Prediction Result:")
        result_label.setFont(QFont("Arial", 10, QFont.Bold))
        layout.addWidget(result_label)
        
        self.result_display = QLabel("Enter job title and description, then click 'Predict'")
        self.result_display.setStyleSheet(
            "background-color: #f0f0f0; padding: 15px; border: 2px solid #ddd; "
            "border-radius: 5px; min-height: 80px;"
        )
        self.result_display.setAlignment(Qt.AlignCenter)
        self.result_display.setWordWrap(True)
        layout.addWidget(self.result_display)
        
        # Load model button
        load_model_btn = QPushButton("Load Model from File...")
        load_model_btn.setStyleSheet(
            "QPushButton { background-color: #2196F3; color: white; "
            "font-weight: bold; padding: 8px; border-radius: 5px; }"
            "QPushButton:hover { background-color: #0b7dda; }"
        )
        load_model_btn.clicked.connect(self.load_model_from_file)
        layout.addWidget(load_model_btn)
    
    def load_model(self):
        """Load model from default path."""
        import joblib
        if MODEL_PATH.exists():
            try:
                self.model = joblib.load(MODEL_PATH)
                self.status_label.setText(f"Model: Loaded from {MODEL_PATH}")
                self.status_label.setStyleSheet("color: green; font-weight: bold;")
                self.predict_btn.setEnabled(True)
            except Exception as e:
                self.status_label.setText(f"Model: Error loading - {str(e)}")
                self.status_label.setStyleSheet("color: red; font-weight: bold;")
        else:
            self.status_label.setText(f"Model: Not found at {MODEL_PATH}")
            self.status_label.setStyleSheet("color: orange; font-weight: bold;")
    
    def load_model_from_file(self):
        """Load model from file dialog."""
        import joblib
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Model File",
            str(Path.cwd()),
            "Joblib Files (*.joblib);;All Files (*)"
        )
        if file_path:
            try:
                self.model = joblib.load(file_path)
                self.status_label.setText(f"Model: Loaded from {file_path}")
                self.status_label.setStyleSheet("color: green; font-weight: bold;")
                self.predict_btn.setEnabled(True)
                QMessageBox.information(self, "Success", "Model loaded successfully!")
            except Exception as e:
                self.status_label.setText(f"Model: Error loading - {str(e)}")
                self.status_label.setStyleSheet("color: red; font-weight: bold;")
                QMessageBox.critical(self, "Error", f"Failed to load model:\n{str(e)}")
    
    def predict(self):
        """Predict experience level."""
        if self.model is None:
            QMessageBox.warning(self, "No Model", "Please load a model first.")
            return
        
        title = self.title_input.toPlainText().strip()
        description = self.desc_input.toPlainText().strip()
        
        if not title and not description:
            QMessageBox.warning(self, "Empty Input", "Please enter at least a job title or description.")
            return
        
        text = f"{title}\n{description}".strip()
        
        try:
            prediction = self.model.predict([text])[0]
            result_text = f"Predicted Experience Level: {prediction.upper()}"
            
            color_map = {
                "intern": "#FF9800",
                "junior": "#4CAF50",
                "mid": "#2196F3",
                "senior": "#9C27B0",
                "lead": "#E91E63",
                "manager": "#F44336",
                "director_plus": "#000000"
            }
            color = color_map.get(prediction.lower(), "#666666")
            
            self.result_display.setText(result_text)
            self.result_display.setStyleSheet(
                f"background-color: #f0f0f0; padding: 15px; border: 3px solid {color}; "
                "border-radius: 5px; min-height: 80px; font-size: 14px; font-weight: bold;"
            )
        except Exception as e:
            QMessageBox.critical(self, "Prediction Error", f"An error occurred:\n{str(e)}")
    
    def clear_inputs(self):
        """Clear all inputs."""
        self.title_input.clear()
        self.desc_input.clear()
        self.result_display.setText("Enter job title and description, then click 'Predict'")
        self.result_display.setStyleSheet(
            "background-color: #f0f0f0; padding: 15px; border: 2px solid #ddd; "
            "border-radius: 5px; min-height: 80px;"
        )


class MainWindow(QMainWindow):
    """Main application window."""
    
    def __init__(self):
        super().__init__()
        self.init_ui()
    
    def init_ui(self):
        self.setWindowTitle("Job Posting Analysis Pipeline")
        self.setGeometry(100, 100, 1200, 800)
        
        # Navbar
        navbar = self.create_navbar()
        self.addToolBar(Qt.TopToolBarArea, navbar)
        
        # Stacked widget for views
        self.stack = QStackedWidget()
        
        # Main view
        self.main_view = MainView()
        self.stack.addWidget(self.main_view)
        
        # Try it out view
        self.try_it_view = TryItOutView()
        self.stack.addWidget(self.try_it_view)
        
        self.setCentralWidget(self.stack)
    
    def create_navbar(self):
        """Create navigation toolbar."""
        navbar = self.addToolBar("Navigation")
        navbar.setMovable(False)
        navbar.setStyleSheet("""
            QToolBar {
                background-color: #2196F3;
                spacing: 5px;
                padding: 5px;
            }
        """)
        
        main_btn = QPushButton("Main")
        main_btn.setStyleSheet(
            "QPushButton { background-color: white; color: #2196F3; "
            "font-weight: bold; padding: 8px 20px; border-radius: 4px; }"
            "QPushButton:hover { background-color: #e3f2fd; }"
        )
        main_btn.clicked.connect(lambda: self.stack.setCurrentIndex(0))
        navbar.addWidget(main_btn)
        
        try_it_btn = QPushButton("Try it out")
        try_it_btn.setStyleSheet(
            "QPushButton { background-color: white; color: #2196F3; "
            "font-weight: bold; padding: 8px 20px; border-radius: 4px; }"
            "QPushButton:hover { background-color: #e3f2fd; }"
        )
        try_it_btn.clicked.connect(lambda: self.stack.setCurrentIndex(1))
        navbar.addWidget(try_it_btn)
        
        return navbar


def main():
    """Main entry point."""
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
