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
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Qt5Agg')  # Use Qt5 backend
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QTextEdit, QPushButton, QMessageBox, QFileDialog,
    QStackedWidget, QListWidget, QListWidgetItem, QFormLayout,
    QLineEdit, QGroupBox, QScrollArea, QFrame, QSplitter, QGridLayout,
    QSizePolicy
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
        
        # Data Processing view (includes input dataset upload)
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
        sidebar.setStyleSheet("""
            QFrame {
                background-color: #1a237e;
                border: none;
            }
        """)
        sidebar_layout = QVBoxLayout()
        sidebar.setLayout(sidebar_layout)
        
        # Sidebar title
        sidebar_title = QLabel("Navigation")
        sidebar_title.setFont(QFont("Arial", 12, QFont.Bold))
        sidebar_title.setStyleSheet("color: white; padding: 10px;")
        sidebar_layout.addWidget(sidebar_title)
        
        # Category list
        self.category_list = QListWidget()
        self.category_list.setStyleSheet("""
            QListWidget {
                background-color: #283593;
                border: none;
                color: white;
            }
            QListWidget::item {
                padding: 12px;
                border-bottom: 1px solid #3949ab;
                color: white;
            }
            QListWidget::item:hover {
                background-color: #3949ab;
            }
            QListWidget::item:selected {
                background-color: #5c6bc0;
                color: white;
            }
        """)
        
        categories = [
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
    
    def create_data_processing_view(self):
        """Create data processing view with dataset upload."""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        # Upload Dataset section
        upload_group = QGroupBox("Upload Dataset")
        upload_group.setFont(QFont("Arial", 12, QFont.Bold))
        upload_form_layout = QFormLayout()
        upload_group.setLayout(upload_form_layout)
        
        # File path display
        self.dataset_path_label = QLabel("No file selected")
        self.dataset_path_label.setStyleSheet("color: #5c6bc0; padding: 5px;")
        
        # Browse button
        browse_btn = QPushButton("Browse CSV File...")
        browse_btn.clicked.connect(self.browse_dataset)
        
        path_layout = QHBoxLayout()
        path_layout.addWidget(self.dataset_path_label, stretch=1)
        path_layout.addWidget(browse_btn)
        upload_form_layout.addRow("Dataset:", path_layout)
        
        # Info
        upload_info_label = QLabel(
            "Upload a CSV file containing job postings.\n"
            "Required columns: job_id, title, description, company_name, etc."
        )
        upload_info_label.setWordWrap(True)
        upload_info_label.setStyleSheet("color: #5c6bc0; padding: 10px;")
        upload_form_layout.addRow("", upload_info_label)
        
        layout.addWidget(upload_group)
        
        # Clean Datasets section
        group = QGroupBox("Clean Datasets")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        group_layout = QVBoxLayout()
        group.setLayout(group_layout)
        
        # PL button
        self.pl_btn = QPushButton("Clean PL Dataset")
        self.pl_btn.clicked.connect(self.clean_pl_dataset)
        group_layout.addWidget(self.pl_btn)
        
        # EN button
        self.en_btn = QPushButton("Clean EN Dataset")
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
            self.dataset_path_label.setStyleSheet("color: #283593; padding: 5px; font-weight: bold;")
    
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
    
    def clean_pl_dataset(self):
        """Run clean_pl_dataset.py script."""
        script_path = Path("data_processing/clean_pl_dataset.py")
        if not script_path.exists():
            QMessageBox.critical(
                self,
                "Script Not Found",
                f"Script not found at: {script_path}"
            )
            return
        
        self.pl_btn.setEnabled(False)
        self.processing_output.clear()
        self.processing_output.append("Starting PL dataset cleaning...")
        
        self.pl_thread = ProcessThread(script_path)
        self.pl_thread.finished.connect(self.on_pl_processing_finished)
        self.pl_thread.start()
    
    def on_pl_processing_finished(self, output, success):
        """Handle PL processing completion."""
        self.pl_btn.setEnabled(True)
        self.processing_output.append(output)
        if success:
            self.processing_output.append("\n✓ PL dataset cleaning completed successfully!")
            QMessageBox.information(self, "Success", "PL dataset cleaning completed!")
        else:
            self.processing_output.append("\n✗ PL processing failed!")
            QMessageBox.warning(self, "Error", f"PL processing failed:\n{output[:500]}")
    
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
        split_info.setStyleSheet("color: #5c6bc0; padding: 5px;")
        split_layout.addWidget(split_info)
        
        self.split_btn = QPushButton("Create Train/Val/Test Split")
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
        baseline_info.setStyleSheet("color: #5c6bc0; padding: 5px;")
        baseline_layout.addWidget(baseline_info)
        
        self.baseline_btn = QPushButton("Train Baseline Model")
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
        self.status_label.setStyleSheet("color: #7986cb; font-weight: bold; padding: 5px;")
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
        self.predict_btn.clicked.connect(self.predict)
        self.predict_btn.setEnabled(False)
        button_layout.addWidget(self.predict_btn)
        
        clear_btn = QPushButton("Clear")
        clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #7986cb;
                color: white;
            }
            QPushButton:hover {
                background-color: #5c6bc0;
            }
        """)
        clear_btn.clicked.connect(self.clear_inputs)
        button_layout.addWidget(clear_btn)
        
        layout.addLayout(button_layout)
        
        # Result
        result_label = QLabel("Prediction Result:")
        result_label.setFont(QFont("Arial", 10, QFont.Bold))
        layout.addWidget(result_label)
        
        self.result_display = QLabel("Enter job title and description, then click 'Predict'")
        self.result_display.setStyleSheet(
            "background-color: #e8eaf6; padding: 15px; border: 2px solid #5c6bc0; "
            "border-radius: 5px; min-height: 80px; color: #1a237e;"
        )
        self.result_display.setAlignment(Qt.AlignCenter)
        self.result_display.setWordWrap(True)
        layout.addWidget(self.result_display)
        
        # Load model button
        load_model_btn = QPushButton("Load Model from File...")
        load_model_btn.clicked.connect(self.load_model_from_file)
        layout.addWidget(load_model_btn)
    
    def load_model(self):
        """Load model from default path."""
        import joblib
        if MODEL_PATH.exists():
            try:
                self.model = joblib.load(MODEL_PATH)
                self.status_label.setText(f"Model: Loaded from {MODEL_PATH}")
                self.status_label.setStyleSheet("color: #283593; font-weight: bold; padding: 5px;")
                self.predict_btn.setEnabled(True)
            except Exception as e:
                self.status_label.setText(f"Model: Error loading - {str(e)}")
                self.status_label.setStyleSheet("color: #7986cb; font-weight: bold; padding: 5px;")
        else:
            self.status_label.setText(f"Model: Not found at {MODEL_PATH}")
            self.status_label.setStyleSheet("color: #9fa8da; font-weight: bold; padding: 5px;")
    
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
                self.status_label.setStyleSheet("color: #283593; font-weight: bold; padding: 5px;")
                self.predict_btn.setEnabled(True)
                QMessageBox.information(self, "Success", "Model loaded successfully!")
            except Exception as e:
                self.status_label.setText(f"Model: Error loading - {str(e)}")
                self.status_label.setStyleSheet("color: #7986cb; font-weight: bold; padding: 5px;")
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
            
            # Navy blue color scheme for different experience levels
            color_map = {
                "intern": "#9fa8da",      # Light indigo
                "junior": "#7986cb",      # Medium indigo
                "mid": "#5c6bc0",        # Indigo
                "senior": "#3949ab",      # Deep indigo
                "lead": "#283593",        # Dark indigo
                "manager": "#1a237e",     # Very dark indigo
                "director_plus": "#0d47a1" # Deepest blue
            }
            color = color_map.get(prediction.lower(), "#5c6bc0")
            
            self.result_display.setText(result_text)
            self.result_display.setStyleSheet(
                f"background-color: #e8eaf6; padding: 15px; border: 3px solid {color}; "
                "border-radius: 5px; min-height: 80px; font-size: 14px; font-weight: bold; "
                f"color: {color};"
            )
        except Exception as e:
            QMessageBox.critical(self, "Prediction Error", f"An error occurred:\n{str(e)}")
    
    def clear_inputs(self):
        """Clear all inputs."""
        self.title_input.clear()
        self.desc_input.clear()
        self.result_display.setText("Enter job title and description, then click 'Predict'")
        self.result_display.setStyleSheet(
            "background-color: #e8eaf6; padding: 15px; border: 2px solid #5c6bc0; "
            "border-radius: 5px; min-height: 80px; color: #1a237e;"
        )


class DatasetStatisticsView(QWidget):
    """View for dataset statistics and visualizations."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.df = None
        self.df_train = None
        self.df_val = None
        self.df_test = None
        self.init_ui()
        self.load_data()
    
    def init_ui(self):
        layout = QVBoxLayout()
        self.setLayout(layout)
        
        # Title
        title = QLabel("Dataset Statistics")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title.setFont(title_font)
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)
        
        # Scroll area for content
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout()
        scroll_layout.setSpacing(20)  # Add spacing between sections
        scroll_widget.setLayout(scroll_layout)
        
        # Load data button
        load_btn = QPushButton("Reload Data")
        load_btn.clicked.connect(self.load_data)
        scroll_layout.addWidget(load_btn)
        
        # Status label
        self.status_label = QLabel("Loading data...")
        self.status_label.setStyleSheet("color: #5c6bc0; padding: 5px;")
        scroll_layout.addWidget(self.status_label)
        
        # Overview section
        self.overview_group = self.create_overview_section()
        scroll_layout.addWidget(self.overview_group)
        
        # Label distribution section
        self.label_group = self.create_label_distribution_section()
        scroll_layout.addWidget(self.label_group)
        
        # Text statistics section
        self.text_group = self.create_text_statistics_section()
        scroll_layout.addWidget(self.text_group)
        
        # Work arrangement section
        self.work_group = self.create_work_arrangement_section()
        scroll_layout.addWidget(self.work_group)
        
        # Time section
        self.time_group = self.create_time_section()
        scroll_layout.addWidget(self.time_group)
        
        scroll_layout.addStretch()
        
        scroll.setWidget(scroll_widget)
        layout.addWidget(scroll)
    
    def load_data(self):
        """Load dataset files."""
        self.status_label.setText("Loading data...")
        self.status_label.setStyleSheet("color: #5c6bc0; padding: 5px;")
        
        try:
            # Try to load cleaned dataset
            clean_path = Path("en_dataset/en_jobs_clean.csv")
            if not clean_path.exists():
                clean_path = Path("data_processing/en_jobs_clean.csv")
            
            if clean_path.exists():
                self.df = pd.read_csv(clean_path, low_memory=False)
                self.status_label.setText(f"Loaded {len(self.df):,} rows from {clean_path}")
                self.status_label.setStyleSheet("color: #283593; padding: 5px; font-weight: bold;")
            else:
                self.status_label.setText("en_jobs_clean.csv not found. Please run data processing first.")
                self.status_label.setStyleSheet("color: #7986cb; padding: 5px;")
                self.df = None
            
            # Try to load splits - check multiple locations
            for split_name, attr_name in [("en_train.csv", "df_train"), 
                                         ("en_val.csv", "df_val"), 
                                         ("en_test.csv", "df_test")]:
                split_path = None
                # Check multiple possible locations
                possible_paths = [
                    Path(f"model_output/{split_name}"),
                    Path(f"data_processing/{split_name}"),
                    Path(f"model/{split_name}")
                ]
                
                for path in possible_paths:
                    if path.exists():
                        split_path = path
                        break
                
                if split_path and split_path.exists():
                    try:
                        setattr(self, attr_name, pd.read_csv(split_path, low_memory=False))
                        print(f"Loaded {split_name} from {split_path}")
                    except Exception as e:
                        print(f"Error loading {split_name}: {e}")
                        setattr(self, attr_name, None)
                else:
                    setattr(self, attr_name, None)
            
            # Update all visualizations
            self.update_overview()
            self.update_label_distribution()
            self.update_text_statistics()
            self.update_work_arrangement()
            self.update_time()
            
        except Exception as e:
            self.status_label.setText(f"Error loading data: {str(e)}")
            self.status_label.setStyleSheet("color: #7986cb; padding: 5px;")
    
    def create_overview_section(self):
        """Create overview/quality gates section."""
        group = QGroupBox("Overview & Quality Gates")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        group.setLayout(layout)
        
        # Metrics grid
        metrics_grid = QGridLayout()
        self.metric_labels = {}
        
        metric_names = [
            "Raw Rows", "Rows with Platform Label", "Rows without Platform Label",
            "Years Hint Coverage", "Title Hint Coverage", "Platform Label Coverage"
        ]
        
        for i, name in enumerate(metric_names):
            label = QLabel(f"{name}:")
            label.setStyleSheet("font-weight: bold; color: #1a237e;")
            value = QLabel("N/A")
            value.setStyleSheet("color: #5c6bc0; padding: 2px;")
            self.metric_labels[name] = value
            
            row = i // 2
            col = (i % 2) * 2
            metrics_grid.addWidget(label, row, col)
            metrics_grid.addWidget(value, row, col + 1)
        
        layout.addLayout(metrics_grid)
        
        # Missingness chart
        missing_label = QLabel("Missing Data by Column:")
        missing_label.setStyleSheet("font-weight: bold; color: #1a237e; margin-top: 10px;")
        layout.addWidget(missing_label)
        
        self.missing_canvas = FigureCanvas(Figure(figsize=(10, 4)))
        self.missing_canvas.setStyleSheet("background-color: white;")
        self.missing_canvas.setFixedHeight(400)
        layout.addWidget(self.missing_canvas)
        
        return group
    
    def create_label_distribution_section(self):
        """Create label distribution section."""
        group = QGroupBox("Label Distribution")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        layout.setSpacing(10)
        group.setLayout(layout)
        
        # Class distribution chart
        self.label_dist_canvas = FigureCanvas(Figure(figsize=(10, 5)))
        self.label_dist_canvas.setStyleSheet("background-color: white;")
        self.label_dist_canvas.setFixedHeight(500)
        layout.addWidget(self.label_dist_canvas)
        
        # Split distribution chart
        split_label = QLabel("Label Distribution by Split:")
        split_label.setStyleSheet("font-weight: bold; color: #1a237e; margin-top: 10px;")
        layout.addWidget(split_label)
        
        self.split_dist_canvas = FigureCanvas(Figure(figsize=(10, 5)))
        self.split_dist_canvas.setStyleSheet("background-color: white;")
        self.split_dist_canvas.setFixedHeight(500)
        layout.addWidget(self.split_dist_canvas)
        
        return group
    
    def create_text_statistics_section(self):
        """Create text statistics section."""
        group = QGroupBox("Text Statistics")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        layout.setSpacing(10)
        group.setLayout(layout)
        
        # Text length histogram
        length_label = QLabel("Description Length Distribution:")
        length_label.setStyleSheet("font-weight: bold; color: #1a237e;")
        layout.addWidget(length_label)
        
        self.text_length_canvas = FigureCanvas(Figure(figsize=(10, 4)))
        self.text_length_canvas.setStyleSheet("background-color: white;")
        self.text_length_canvas.setFixedHeight(400)
        layout.addWidget(self.text_length_canvas)
        
        # Text length by class
        by_class_label = QLabel("Description Length by Experience Level:")
        by_class_label.setStyleSheet("font-weight: bold; color: #1a237e; margin-top: 10px;")
        layout.addWidget(by_class_label)
        
        self.text_by_class_canvas = FigureCanvas(Figure(figsize=(10, 5)))
        self.text_by_class_canvas.setStyleSheet("background-color: white;")
        self.text_by_class_canvas.setFixedHeight(500)
        layout.addWidget(self.text_by_class_canvas)
        
        return group
    
    def create_work_arrangement_section(self):
        """Create work arrangement & compensation section."""
        group = QGroupBox("Work Arrangement & Compensation")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        layout.setSpacing(10)
        group.setLayout(layout)
        
        # Work type distribution
        work_type_label = QLabel("Work Type Distribution:")
        work_type_label.setStyleSheet("font-weight: bold; color: #1a237e;")
        layout.addWidget(work_type_label)
        
        self.work_type_canvas = FigureCanvas(Figure(figsize=(10, 5)))
        self.work_type_canvas.setStyleSheet("background-color: white;")
        self.work_type_canvas.setFixedHeight(500)
        layout.addWidget(self.work_type_canvas)
        
        # Remote distribution
        remote_label = QLabel("Remote Allowed Distribution:")
        remote_label.setStyleSheet("font-weight: bold; color: #1a237e; margin-top: 10px;")
        layout.addWidget(remote_label)
        
        self.remote_canvas = FigureCanvas(Figure(figsize=(8, 4)))
        self.remote_canvas.setStyleSheet("background-color: white;")
        self.remote_canvas.setFixedHeight(400)
        layout.addWidget(self.remote_canvas)
        
        # Salary distribution
        salary_label = QLabel("Salary Distribution (Annual):")
        salary_label.setStyleSheet("font-weight: bold; color: #1a237e; margin-top: 10px;")
        layout.addWidget(salary_label)
        
        self.salary_canvas = FigureCanvas(Figure(figsize=(10, 5)))
        self.salary_canvas.setStyleSheet("background-color: white;")
        self.salary_canvas.setFixedHeight(500)
        layout.addWidget(self.salary_canvas)
        
        return group
    
    def create_time_section(self):
        """Create time/drift section."""
        group = QGroupBox("Time Series / Drift")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        layout.setSpacing(10)
        group.setLayout(layout)
        
        self.time_canvas = FigureCanvas(Figure(figsize=(10, 4)))
        self.time_canvas.setStyleSheet("background-color: white;")
        self.time_canvas.setFixedHeight(400)
        layout.addWidget(self.time_canvas)
        
        return group
    
    def update_overview(self):
        """Update overview metrics and charts."""
        if self.df is None:
            return
        
        # Calculate metrics
        raw_rows = len(self.df)
        rows_with_label = self.df["platform_experience_label"].notna().sum() if "platform_experience_label" in self.df.columns else 0
        rows_without_label = raw_rows - rows_with_label
        
        cov_years = self.df["years_hint"].notna().mean() * 100 if "years_hint" in self.df.columns else 0
        cov_title = self.df["title_hint"].notna().mean() * 100 if "title_hint" in self.df.columns else 0
        cov_platform = self.df["platform_experience_label"].notna().mean() * 100 if "platform_experience_label" in self.df.columns else 0
        
        # Update labels
        self.metric_labels["Raw Rows"].setText(f"{raw_rows:,}")
        self.metric_labels["Rows with Platform Label"].setText(f"{rows_with_label:,}")
        self.metric_labels["Rows without Platform Label"].setText(f"{rows_without_label:,}")
        self.metric_labels["Years Hint Coverage"].setText(f"{cov_years:.1f}%")
        self.metric_labels["Title Hint Coverage"].setText(f"{cov_title:.1f}%")
        self.metric_labels["Platform Label Coverage"].setText(f"{cov_platform:.1f}%")
        
        # Missingness chart
        key_cols = ["title", "description_clean", "salary_annual_min", "salary_annual_max", 
                   "pay_period", "work_type", "remote_allowed", "location"]
        missing_data = {}
        for col in key_cols:
            if col in self.df.columns:
                missing_data[col] = (1 - self.df[col].notna().mean()) * 100
        
        if missing_data:
            fig = self.missing_canvas.figure
            fig.clear()
            ax = fig.add_subplot(111)
            cols = list(missing_data.keys())
            values = list(missing_data.values())
            ax.barh(cols, values, color='#5c6bc0')
            ax.set_xlabel('% Missing')
            ax.set_title('Missing Data by Column')
            ax.set_xlim(0, 100)
            fig.tight_layout()
            self.missing_canvas.draw()
    
    def update_label_distribution(self):
        """Update label distribution charts."""
        if self.df is None or "platform_experience_label" not in self.df.columns:
            return
        
        silver = self.df[self.df["platform_experience_label"].notna()].copy()
        if len(silver) == 0:
            return
        
        # Class distribution
        label_counts = silver["platform_experience_label"].value_counts()
        label_props = silver["platform_experience_label"].value_counts(normalize=True) * 100
        
        fig = self.label_dist_canvas.figure
        fig.clear()
        ax = fig.add_subplot(111)
        ax.bar(label_counts.index, label_counts.values, color='#3949ab')
        ax.set_xlabel('Experience Level')
        ax.set_ylabel('Count')
        ax.set_title('Class Distribution (Counts)')
        ax.tick_params(axis='x', rotation=45)
        fig.tight_layout()
        self.label_dist_canvas.draw()
        
        # Split distribution
        fig = self.split_dist_canvas.figure
        fig.clear()
        ax = fig.add_subplot(111)
        
        if self.df_train is not None and self.df_val is not None and self.df_test is not None:
            splits = {
                'Train': self.df_train,
                'Val': self.df_val,
                'Test': self.df_test
            }
            
            # Get all unique labels from all splits
            all_labels = set()
            for split_df in splits.values():
                if "platform_experience_label" in split_df.columns:
                    all_labels.update(split_df["platform_experience_label"].dropna().unique())
            
            if len(all_labels) > 0:
                labels = sorted(list(all_labels))
                x = np.arange(len(labels))
                width = 0.25
                
                colors = ['#3949ab', '#5c6bc0', '#7986cb']
                for i, (split_name, split_df) in enumerate(splits.items()):
                    if "platform_experience_label" in split_df.columns:
                        split_counts = split_df["platform_experience_label"].value_counts()
                        values = [split_counts.get(label, 0) for label in labels]
                        ax.bar(x + i * width, values, width, label=split_name, color=colors[i])
                
                ax.set_xlabel('Experience Level')
                ax.set_ylabel('Count')
                ax.set_title('Label Distribution by Split')
                ax.set_xticks(x + width)
                ax.set_xticklabels(labels, rotation=45)
                ax.legend()
            else:
                ax.text(0.5, 0.5, 'No label data in splits', 
                       ha='center', va='center', transform=ax.transAxes,
                       fontsize=12, color='#5c6bc0')
                ax.set_title('Label Distribution by Split - No Data')
        else:
            ax.text(0.5, 0.5, 'Split files not found.\nPlease run "Create Train/Val/Test Split" first.', 
                   ha='center', va='center', transform=ax.transAxes,
                   fontsize=12, color='#5c6bc0')
            ax.set_title('Label Distribution by Split - Files Not Found')
        
        fig.tight_layout()
        self.split_dist_canvas.draw()
    
    def update_text_statistics(self):
        """Update text statistics charts."""
        if self.df is None or "platform_experience_label" not in self.df.columns:
            return
        
        silver = self.df[self.df["platform_experience_label"].notna()].copy()
        if len(silver) == 0:
            return
        
        # Calculate word count if description_clean exists
        if "description_clean" in silver.columns:
            silver["word_count"] = silver["description_clean"].fillna("").astype(str).str.split().str.len()
        elif "desc_len" in silver.columns:
            silver["word_count"] = silver["desc_len"]
        else:
            return
        
        # Text length histogram
        fig = self.text_length_canvas.figure
        fig.clear()
        ax = fig.add_subplot(111)
        ax.hist(silver["word_count"], bins=50, color='#5c6bc0', edgecolor='#3949ab')
        ax.set_xlabel('Word Count')
        ax.set_ylabel('Frequency')
        ax.set_title('Description Length Distribution')
        ax.set_yscale('log')
        fig.tight_layout()
        self.text_length_canvas.draw()
        
        # Text length by class
        fig = self.text_by_class_canvas.figure
        fig.clear()
        ax = fig.add_subplot(111)
        
        labels = silver["platform_experience_label"].unique()
        data_by_label = [silver[silver["platform_experience_label"] == label]["word_count"].values 
                        for label in labels]
        
        bp = ax.boxplot(data_by_label, labels=labels, patch_artist=True)
        for patch in bp['boxes']:
            patch.set_facecolor('#5c6bc0')
        
        ax.set_xlabel('Experience Level')
        ax.set_ylabel('Word Count')
        ax.set_title('Description Length by Experience Level')
        ax.tick_params(axis='x', rotation=45)
        fig.tight_layout()
        self.text_by_class_canvas.draw()
    
    def update_work_arrangement(self):
        """Update work arrangement charts."""
        if self.df is None:
            return
        
        silver = self.df[self.df["platform_experience_label"].notna()].copy() if "platform_experience_label" in self.df.columns else self.df.copy()
        
        # Work type distribution
        if "work_type" in silver.columns:
            work_counts = silver["work_type"].value_counts()
            fig = self.work_type_canvas.figure
            fig.clear()
            ax = fig.add_subplot(111)
            ax.bar(work_counts.index, work_counts.values, color='#3949ab')
            ax.set_xlabel('Work Type')
            ax.set_ylabel('Count')
            ax.set_title('Work Type Distribution')
            ax.tick_params(axis='x', rotation=45)
            fig.tight_layout()
            self.work_type_canvas.draw()
        
        # Remote distribution
        if "remote_allowed" in silver.columns:
            remote_counts = silver["remote_allowed"].value_counts()
            fig = self.remote_canvas.figure
            fig.clear()
            ax = fig.add_subplot(111)
            ax.bar(['Not Remote', 'Remote'], [remote_counts.get(0, 0), remote_counts.get(1, 0)], 
                  color=['#7986cb', '#5c6bc0'])
            ax.set_ylabel('Count')
            ax.set_title('Remote Allowed Distribution')
            fig.tight_layout()
            self.remote_canvas.draw()
        
        # Salary distribution
        if "salary_annual_min" in silver.columns and "salary_annual_max" in silver.columns:
            silver["salary_mid"] = np.where(
                silver["salary_annual_min"].notna() & silver["salary_annual_max"].notna(),
                (silver["salary_annual_min"] + silver["salary_annual_max"]) / 2,
                np.nan
            )
            salary_data = silver["salary_mid"].dropna()
            if len(salary_data) > 0:
                # Filter outliers (99th percentile)
                p99 = salary_data.quantile(0.99)
                salary_data = salary_data[salary_data <= p99]
                
                fig = self.salary_canvas.figure
                fig.clear()
                ax = fig.add_subplot(111)
                ax.hist(salary_data, bins=50, color='#5c6bc0', edgecolor='#3949ab')
                ax.set_xlabel('Annual Salary (USD)')
                ax.set_ylabel('Frequency')
                ax.set_title('Salary Distribution (Annual, < 99th percentile)')
                ax.set_yscale('log')
                fig.tight_layout()
                self.salary_canvas.draw()
    
    def update_time(self):
        """Update time series chart."""
        if self.df is None or "original_listed_time" not in self.df.columns:
            return
        
        silver = self.df[self.df["platform_experience_label"].notna()].copy() if "platform_experience_label" in self.df.columns else self.df.copy()
        
        try:
            t = pd.to_datetime(silver["original_listed_time"], unit="ms", errors="coerce")
            silver["month"] = t.dt.to_period("M").astype(str)
            posts_per_month = silver["month"].value_counts().sort_index()
            
            if len(posts_per_month) > 0:
                fig = self.time_canvas.figure
                fig.clear()
                ax = fig.add_subplot(111)
                ax.plot(posts_per_month.index, posts_per_month.values, 
                       marker='o', color='#3949ab', linewidth=2, markersize=4)
                ax.set_xlabel('Month')
                ax.set_ylabel('Number of Postings')
                ax.set_title('Job Postings Over Time')
                ax.tick_params(axis='x', rotation=45)
                fig.tight_layout()
                self.time_canvas.draw()
        except Exception as e:
            pass  # Skip if time conversion fails


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
        
        # Dataset Statistics view
        self.stats_view = DatasetStatisticsView()
        self.stack.addWidget(self.stats_view)
        
        self.setCentralWidget(self.stack)
    
    def create_navbar(self):
        """Create navigation toolbar."""
        navbar = self.addToolBar("Navigation")
        navbar.setMovable(False)
        navbar.setStyleSheet("""
            QToolBar {
                background-color: #1a237e;
                spacing: 0px;
                padding: 0px;
                border: none;
            }
        """)
        
        # Create container widget with horizontal layout
        container = QWidget()
        container_layout = QHBoxLayout()
        container_layout.setContentsMargins(0, 0, 0, 0)
        container_layout.setSpacing(0)
        container.setLayout(container_layout)
        
        main_btn = QPushButton("Main")
        main_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        main_btn.setStyleSheet("""
            QPushButton {
                background-color: #283593;
                color: white;
                font-weight: bold;
                padding: 12px 20px;
                border-radius: 0px;
                border-right: 1px solid #1a237e;
            }
            QPushButton:hover {
                background-color: #3949ab;
            }
            QPushButton:pressed {
                background-color: #1a237e;
            }
        """)
        main_btn.clicked.connect(lambda: self.stack.setCurrentIndex(0))
        container_layout.addWidget(main_btn)
        
        try_it_btn = QPushButton("Try it out")
        try_it_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        try_it_btn.setStyleSheet("""
            QPushButton {
                background-color: #283593;
                color: white;
                font-weight: bold;
                padding: 12px 20px;
                border-radius: 0px;
                border-right: 1px solid #1a237e;
            }
            QPushButton:hover {
                background-color: #3949ab;
            }
            QPushButton:pressed {
                background-color: #1a237e;
            }
        """)
        try_it_btn.clicked.connect(lambda: self.stack.setCurrentIndex(1))
        container_layout.addWidget(try_it_btn)
        
        stats_btn = QPushButton("Dataset Statistics")
        stats_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        stats_btn.setStyleSheet("""
            QPushButton {
                background-color: #283593;
                color: white;
                font-weight: bold;
                padding: 12px 20px;
                border-radius: 0px;
            }
            QPushButton:hover {
                background-color: #3949ab;
            }
            QPushButton:pressed {
                background-color: #1a237e;
            }
        """)
        stats_btn.clicked.connect(lambda: self.stack.setCurrentIndex(2))
        container_layout.addWidget(stats_btn)
        
        navbar.addWidget(container)
        
        return navbar


def main():
    """Main entry point."""
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    # Professional navy blue color scheme - independent of system theme
    app.setStyleSheet("""
        QMainWindow {
            background-color: #f5f6fa;
        }
        QWidget {
            background-color: #f5f6fa;
            color: #1a237e;
        }
        QGroupBox {
            font-weight: bold;
            border: 2px solid #3949ab;
            border-radius: 8px;
            margin-top: 10px;
            padding-top: 15px;
            background-color: #ffffff;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 10px;
            padding: 0 5px;
            color: #1a237e;
        }
        QLabel {
            color: #1a237e;
        }
        QTextEdit, QLineEdit {
            background-color: #ffffff;
            border: 2px solid #5c6bc0;
            border-radius: 4px;
            padding: 5px;
            color: #1a237e;
        }
        QTextEdit:focus, QLineEdit:focus {
            border: 2px solid #283593;
        }
        QPushButton {
            background-color: #283593;
            color: white;
            font-weight: bold;
            padding: 10px 20px;
            border: none;
            border-radius: 6px;
        }
        QPushButton:hover {
            background-color: #3949ab;
        }
        QPushButton:pressed {
            background-color: #1a237e;
        }
        QPushButton:disabled {
            background-color: #9fa8da;
            color: #e8eaf6;
        }
        QScrollArea {
            background-color: #f5f6fa;
            border: none;
        }
        QFrame {
            background-color: #ffffff;
        }
    """)
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
