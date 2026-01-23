import sys
import subprocess
import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Qt5Agg')
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QTextEdit, QPushButton, QMessageBox, QFileDialog,
    QStackedWidget, QListWidget, QListWidgetItem, QFormLayout,
    QLineEdit, QGroupBox, QScrollArea, QFrame, QSplitter, QGridLayout,
    QSizePolicy, QComboBox
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont
import json
try:
    import seaborn as sns
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False
from PyQt5.QtGui import QFont, QIcon

MODEL_PATH = Path("model_output/baseline_tfidf_linearsvc.joblib")


class ProcessThread(QThread):
    finished = pyqtSignal(str, bool)
    
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
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()
    
    def init_ui(self):
        layout = QHBoxLayout()
        self.setLayout(layout)
        
        content_area = QScrollArea()
        content_widget = QWidget()
        content_layout = QVBoxLayout()
        content_widget.setLayout(content_layout)
        
        title = QLabel("Data Processing & Model Training")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title.setFont(title_font)
        title.setAlignment(Qt.AlignCenter)
        content_layout.addWidget(title)
        
        self.content_stack = QStackedWidget()
        
        processing_view = self.create_data_processing_view()
        self.content_stack.addWidget(processing_view)
        
        model_view = self.create_model_training_view()
        self.content_stack.addWidget(model_view)
        
        self.model_perf_view = ModelPerformanceView()
        self.content_stack.addWidget(self.model_perf_view)
        
        content_layout.addWidget(self.content_stack)
        content_layout.addStretch()
        
        content_area.setWidget(content_widget)
        content_area.setWidgetResizable(True)
        
        sidebar = self.create_sidebar()
        sidebar.setMaximumWidth(300)
        
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
        
        sidebar_title = QLabel("Navigation")
        sidebar_title.setFont(QFont("Arial", 12, QFont.Bold))
        sidebar_title.setStyleSheet("color: white; padding: 10px;")
        sidebar_layout.addWidget(sidebar_title)
        
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
            "Model Training",
            "Model Performance"
        ]
        
        for cat in categories:
            item = QListWidgetItem(cat)
            self.category_list.addItem(item)
        
        sidebar_layout.addWidget(self.category_list)
        
        return sidebar
    
    def switch_content(self, index):
        self.content_stack.setCurrentIndex(index)
    
    def create_data_processing_view(self):
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        upload_group = QGroupBox("Upload Dataset")
        upload_group.setFont(QFont("Arial", 12, QFont.Bold))
        upload_form_layout = QFormLayout()
        upload_group.setLayout(upload_form_layout)
        
        self.dataset_path_label = QLabel("No file selected")
        self.dataset_path_label.setStyleSheet("color: #5c6bc0; padding: 5px;")
        
        browse_btn = QPushButton("Browse CSV File...")
        browse_btn.clicked.connect(self.browse_dataset)
        
        path_layout = QHBoxLayout()
        path_layout.addWidget(self.dataset_path_label, stretch=1)
        path_layout.addWidget(browse_btn)
        upload_form_layout.addRow("Dataset:", path_layout)
        
        upload_info_label = QLabel(
            "Upload a CSV file containing job postings.\n"
            "Required columns: job_id, title, description, company_name, etc."
        )
        upload_info_label.setWordWrap(True)
        upload_info_label.setStyleSheet("color: #5c6bc0; padding: 10px;")
        upload_form_layout.addRow("", upload_info_label)
        
        layout.addWidget(upload_group)
        
        group = QGroupBox("Clean Datasets")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        group_layout = QVBoxLayout()
        group.setLayout(group_layout)
        
        self.pl_btn = QPushButton("Clean PL Dataset")
        self.pl_btn.clicked.connect(self.clean_pl_dataset)
        group_layout.addWidget(self.pl_btn)
        
        self.en_btn = QPushButton("Clean EN Dataset")
        self.en_btn.clicked.connect(self.clean_en_dataset)
        group_layout.addWidget(self.en_btn)
        
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
        self.pl_btn.setEnabled(True)
        self.processing_output.append(output)
        if success:
            self.processing_output.append("\n✓ PL dataset cleaning completed successfully!")
            QMessageBox.information(self, "Success", "PL dataset cleaning completed!")
        else:
            self.processing_output.append("\n✗ PL processing failed!")
            QMessageBox.warning(self, "Error", f"PL processing failed:\n{output[:500]}")
    
    def on_processing_finished(self, output, success):
        self.en_btn.setEnabled(True)
        self.processing_output.append(output)
        if success:
            self.processing_output.append("\n✓ Processing completed successfully!")
            QMessageBox.information(self, "Success", "EN dataset cleaning completed!")
        else:
            self.processing_output.append("\n✗ Processing failed!")
            QMessageBox.warning(self, "Error", f"Processing failed:\n{output[:500]}")
    
    def create_model_training_view(self):
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        split_group = QGroupBox("Train Split")
        split_group.setFont(QFont("Arial", 12, QFont.Bold))
        split_layout = QVBoxLayout()
        split_group.setLayout(split_layout)
        
        split_info = QLabel(
            "Create train/validation/test splits from cleaned dataset.\n"
            "Output: {lang}_train.csv, {lang}_val.csv, {lang}_test.csv"
        )
        split_info.setWordWrap(True)
        split_info.setStyleSheet("color: #5c6bc0; padding: 5px;")
        split_layout.addWidget(split_info)
        
        split_lang_layout = QHBoxLayout()
        split_lang_label = QLabel("Dataset:")
        split_lang_label.setStyleSheet("font-weight: bold; color: #1a237e;")
        self.split_lang_combo = QComboBox()
        self.split_lang_combo.addItems(["EN (English)", "PL (Polish)"])
        split_lang_layout.addWidget(split_lang_label)
        split_lang_layout.addWidget(self.split_lang_combo)
        split_lang_layout.addStretch()
        split_layout.addLayout(split_lang_layout)
        
        self.split_btn = QPushButton("Create Train/Val/Test Split")
        self.split_btn.clicked.connect(self.create_train_split)
        split_layout.addWidget(self.split_btn)
        
        self.split_output = QTextEdit()
        self.split_output.setReadOnly(True)
        self.split_output.setMaximumHeight(150)
        split_layout.addWidget(QLabel("Output:"))
        split_layout.addWidget(self.split_output)
        
        layout.addWidget(split_group)
        
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
        
        baseline_lang_layout = QHBoxLayout()
        baseline_lang_label = QLabel("Dataset:")
        baseline_lang_label.setStyleSheet("font-weight: bold; color: #1a237e;")
        self.baseline_lang_combo = QComboBox()
        self.baseline_lang_combo.addItems(["EN (English)", "PL (Polish)"])
        baseline_lang_layout.addWidget(baseline_lang_label)
        baseline_lang_layout.addWidget(self.baseline_lang_combo)
        baseline_lang_layout.addStretch()
        baseline_layout.addLayout(baseline_lang_layout)
        
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
        script_path = Path("model/make_train_split.py")
        if not script_path.exists():
            QMessageBox.critical(
                self,
                "Script Not Found",
                f"Script not found at: {script_path}"
            )
            return
        
        lang = "en" if self.split_lang_combo.currentIndex() == 0 else "pl"
        
        self.split_btn.setEnabled(False)
        self.split_output.clear()
        self.split_output.append(f"Creating train/val/test split for {lang.upper()}...")
        
        self.split_thread = ProcessThread(script_path, args=[lang])
        self.split_thread.finished.connect(self.on_split_finished)
        self.split_thread.start()
    
    def on_split_finished(self, output, success):
        self.split_btn.setEnabled(True)
        self.split_output.append(output)
        if success:
            self.split_output.append("\n✓ Split completed successfully!")
            QMessageBox.information(self, "Success", "Train/val/test split created!")
        else:
            self.split_output.append("\n✗ Split failed!")
            QMessageBox.warning(self, "Error", f"Split failed:\n{output[:500]}")
    
    def train_baseline(self):
        script_path = Path("model/train_baseline_tfidf.py")
        if not script_path.exists():
            QMessageBox.critical(
                self,
                "Script Not Found",
                f"Script not found at: {script_path}"
            )
            return
        
        lang = "en" if self.baseline_lang_combo.currentIndex() == 0 else "pl"
        
        self.baseline_btn.setEnabled(False)
        self.baseline_output.clear()
        self.baseline_output.append(f"Training baseline model for {lang.upper()}...")
        
        self.baseline_thread = ProcessThread(script_path, args=[lang])
        self.baseline_thread.finished.connect(self.on_baseline_finished)
        self.baseline_thread.start()
    
    def on_baseline_finished(self, output, success):
        self.baseline_btn.setEnabled(True)
        self.baseline_output.append(output)
        if success:
            self.baseline_output.append("\n✓ Model training completed successfully!")
            QMessageBox.information(self, "Success", "Baseline model trained and saved!")
        else:
            self.baseline_output.append("\n✗ Training failed!")
            QMessageBox.warning(self, "Error", f"Training failed:\n{output[:500]}")


class TryItOutView(QWidget):
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.model = None
        self.lang = "en"
        self.test_cases = {}
        self.init_ui()
        self.load_model()
        self.load_test_cases()
    
    def init_ui(self):
        layout = QVBoxLayout()
        self.setLayout(layout)
        
        title = QLabel("Try It Out - Experience Level Predictor")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title.setFont(title_font)
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)
        
        lang_layout = QHBoxLayout()
        lang_label = QLabel("Model Language:")
        lang_label.setFont(QFont("Arial", 10, QFont.Bold))
        lang_label.setStyleSheet("color: #1a237e;")
        self.lang_combo = QComboBox()
        self.lang_combo.addItems(["EN (English)", "PL (Polish)"])
        self.lang_combo.setCurrentIndex(0)
        self.lang_combo.currentIndexChanged.connect(self.on_lang_changed)
        lang_layout.addWidget(lang_label)
        lang_layout.addWidget(self.lang_combo)
        lang_layout.addStretch()
        layout.addLayout(lang_layout)
        
        self.status_label = QLabel("Model: Not loaded")
        self.status_label.setStyleSheet("color: #7986cb; font-weight: bold; padding: 5px;")
        layout.addWidget(self.status_label)
        
        test_prompts_group = QGroupBox("Test Prompts")
        test_prompts_group.setFont(QFont("Arial", 10, QFont.Bold))
        test_prompts_layout = QVBoxLayout()
        test_prompts_group.setLayout(test_prompts_layout)
        
        test_prompts_label = QLabel("Select a test case:")
        test_prompts_label.setStyleSheet("font-weight: bold; color: #1a237e;")
        test_prompts_layout.addWidget(test_prompts_label)
        
        self.test_cases_combo = QComboBox()
        self.test_cases_combo.currentIndexChanged.connect(self.on_test_case_selected)
        test_prompts_layout.addWidget(self.test_cases_combo)
        
        layout.addWidget(test_prompts_group)
        
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
        
        load_model_btn = QPushButton("Load Model from File...")
        load_model_btn.clicked.connect(self.load_model_from_file)
        layout.addWidget(load_model_btn)
    
    def parse_test_prompts(self, lang):
        file_path = Path(f"test_prompts/{lang}_tests.json")
        test_cases = {}
        
        if not file_path.exists():
            return test_cases
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            for item in data:
                case_id = item.get("id", "")
                if case_id:
                    test_cases[case_id] = {
                        "title": item.get("title", ""),
                        "desc": item.get("description", ""),
                        "expected": item.get("expected", "")
                    }
        except Exception as e:
            print(f"Error parsing test prompts JSON: {e}")
        
        return test_cases
    
    def load_test_cases(self):
        self.test_cases = self.parse_test_prompts(self.lang)
        
        self.test_cases_combo.clear()
        if self.test_cases:
            for case_name in sorted(self.test_cases.keys()):
                case_data = self.test_cases[case_name]
                display_name = case_name
                if case_data.get("expected"):
                    display_name += f" (expected: {case_data['expected']})"
                self.test_cases_combo.addItem(display_name, case_name)
        else:
            self.test_cases_combo.addItem("No test cases available", None)
    
    def on_test_case_selected(self, index):
        if index < 0 or not self.test_cases:
            return
        
        case_name = self.test_cases_combo.itemData(index)
        if not case_name or case_name not in self.test_cases:
            return
        
        case_data = self.test_cases[case_name]
        
        self.title_input.setPlainText(case_data.get("title", ""))
        self.desc_input.setPlainText(case_data.get("desc", ""))
    
    def on_lang_changed(self, index):
        self.lang = "en" if index == 0 else "pl"
        self.load_model()
        self.load_test_cases()
    
    def load_model(self):
        import joblib
        
        if self.lang == "pl":
            model_path = Path("model_output/baseline_tfidf_linearsvc_pl.joblib")
        else:
            model_path = Path("model_output/baseline_tfidf_linearsvc.joblib")
        
        if model_path.exists():
            try:
                self.model = joblib.load(model_path)
                self.status_label.setText(f"Model ({self.lang.upper()}): Loaded from {model_path}")
                self.status_label.setStyleSheet("color: #283593; font-weight: bold; padding: 5px;")
                self.predict_btn.setEnabled(True)
            except Exception as e:
                self.status_label.setText(f"Model ({self.lang.upper()}): Error loading - {str(e)}")
                self.status_label.setStyleSheet("color: #7986cb; font-weight: bold; padding: 5px;")
                self.predict_btn.setEnabled(False)
        else:
            self.status_label.setText(f"Model ({self.lang.upper()}): Not found at {model_path}")
            self.status_label.setStyleSheet("color: #9fa8da; font-weight: bold; padding: 5px;")
            self.predict_btn.setEnabled(False)
    
    def load_model_from_file(self):
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
    
    def extract_years_bucket(self, text: str) -> str:
        import re
        matches = re.findall(r"(\d+)\s*(?:\+?\s*)?(?:years?|yrs?)\b", text.lower())
        if not matches:
            return "YEARS_NONE"
        
        max_years = max([int(m) for m in matches])
        
        if max_years <= 1:
            return "YEARS_0_1"
        elif max_years <= 3:
            return "YEARS_2_3"
        elif max_years <= 5:
            return "YEARS_4_5"
        elif max_years <= 8:
            return "YEARS_6_8"
        else:
            return "YEARS_9_PLUS"
    
    def get_topk_predictions(self, model, text, k=3):
        import numpy as np
        try:
            if hasattr(model, 'decision_function'):
                scores = model.decision_function([text])[0]
                if hasattr(model, 'named_steps'):
                    svm = model.named_steps.get('svm', None)
                    if svm is not None and hasattr(svm, 'classes_'):
                        classes = svm.classes_
                    else:
                        classes = model.classes_ if hasattr(model, 'classes_') else None
                else:
                    classes = model.classes_ if hasattr(model, 'classes_') else None
                
                if classes is not None and len(scores) == len(classes):
                    pairs = list(zip(classes, scores))
                    pairs.sort(key=lambda x: x[1], reverse=True)
                    return pairs[:k]
        except Exception:
            pass
        
        try:
            if hasattr(model, 'predict_proba'):
                proba = model.predict_proba([text])[0]
                if hasattr(model, 'classes_'):
                    classes = model.classes_
                elif hasattr(model, 'named_steps'):
                    clf = model.named_steps.get('svm', None) or model.named_steps.get('classifier', None)
                    if clf is not None and hasattr(clf, 'classes_'):
                        classes = clf.classes_
                    else:
                        return None
                else:
                    return None
                
                pairs = list(zip(classes, proba))
                pairs.sort(key=lambda x: x[1], reverse=True)
                return pairs[:k]
        except Exception:
            pass
        
        return None
    
    def predict(self):
        if self.model is None:
            QMessageBox.warning(self, "No Model", "Please load a model first.")
            return
        
        title = self.title_input.toPlainText().strip()
        description = self.desc_input.toPlainText().strip()
        
        if not title and not description:
            QMessageBox.warning(self, "Empty Input", "Please enter at least a job title or description.")
            return
        
        combined_text = f"{title} {description}".strip()
        years_bucket = self.extract_years_bucket(combined_text)
        
        text = f"{title}\n{description}\n{years_bucket}".strip()
        
        word_count = len(text.split())
        is_short = word_count < 25
        
        try:
            prediction = self.model.predict([text])[0]
            
            topk = self.get_topk_predictions(self.model, text, k=3)
            
            result_parts = [f"Predicted Experience Level: {prediction.upper()}"]
            
            
            if topk:
                result_parts.append("\n\nTop 3 Predictions (decision scores):")
                for i, (label, score) in enumerate(topk, 1):
                    score_str = f"{score:.2f}"
                    result_parts.append(f"{i}. {label.upper()}: {score_str}")
            
            result_text = "\n".join(result_parts)
            
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
            
            border_color = "#ff9800" if is_short else color
            
            self.result_display.setText(result_text)
            self.result_display.setStyleSheet(
                f"background-color: #e8eaf6; padding: 15px; border: 3px solid {border_color}; "
                "border-radius: 5px; min-height: 80px; font-size: 14px; font-weight: bold; "
                f"color: {color};"
            )
        except Exception as e:
            QMessageBox.critical(self, "Prediction Error", f"An error occurred:\n{str(e)}")
    
    def clear_inputs(self):
        self.title_input.clear()
        self.desc_input.clear()
        self.result_display.setText("Enter job title and description, then click 'Predict'")
        self.result_display.setStyleSheet(
            "background-color: #e8eaf6; padding: 15px; border: 2px solid #5c6bc0; "
            "border-radius: 5px; min-height: 80px; color: #1a237e;"
        )


class DatasetStatisticsView(QWidget):
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.df = None
        self.df_train = None
        self.df_val = None
        self.df_test = None
        self.dataset_type = "EN"
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
        
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout()
        scroll_layout.setSpacing(20)
        scroll_widget.setLayout(scroll_layout)
        
        selector_layout = QHBoxLayout()
        selector_label = QLabel("Dataset:")
        selector_label.setStyleSheet("font-weight: bold; color: #1a237e;")
        self.dataset_combo = QComboBox()
        self.dataset_combo.addItems(["EN (English)", "PL (Polish)"])
        self.dataset_combo.currentIndexChanged.connect(self.on_dataset_changed)
        selector_layout.addWidget(selector_label)
        selector_layout.addWidget(self.dataset_combo)
        selector_layout.addStretch()
        scroll_layout.addLayout(selector_layout)
        
        load_btn = QPushButton("Reload Data")
        load_btn.clicked.connect(self.load_data)
        scroll_layout.addWidget(load_btn)
        
        self.status_label = QLabel("Loading data...")
        self.status_label.setStyleSheet("color: #5c6bc0; padding: 5px;")
        scroll_layout.addWidget(self.status_label)
        
        self.overview_group = self.create_overview_section()
        scroll_layout.addWidget(self.overview_group)
        
        self.label_group = self.create_label_distribution_section()
        scroll_layout.addWidget(self.label_group)
        
        self.text_group = self.create_text_statistics_section()
        scroll_layout.addWidget(self.text_group)
        
        self.work_group = self.create_work_arrangement_section()
        scroll_layout.addWidget(self.work_group)
        
        scroll_layout.addStretch()
        
        scroll.setWidget(scroll_widget)
        layout.addWidget(scroll)
    
    def on_dataset_changed(self, index):
        self.dataset_type = "EN" if index == 0 else "PL"
        self.load_data()
    
    def load_data(self):
        self.status_label.setText("Loading data...")
        self.status_label.setStyleSheet("color: #5c6bc0; padding: 5px;")
        
        try:
            if self.dataset_type == "EN":
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
                
                for split_name, attr_name in [("en_train.csv", "df_train"), 
                                             ("en_val.csv", "df_val"), 
                                             ("en_test.csv", "df_test")]:
                    split_path = None
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
            else:
                clean_path = Path("pl_dataset/pl_jobs_clean.csv")
                if not clean_path.exists():
                    clean_path = Path("data_processing/pl_jobs_clean.csv")
                
                if clean_path.exists():
                    self.df = pd.read_csv(clean_path, low_memory=False)
                    self.status_label.setText(f"Loaded {len(self.df):,} rows from {clean_path}")
                    self.status_label.setStyleSheet("color: #283593; padding: 5px; font-weight: bold;")
                else:
                    self.status_label.setText("pl_jobs_clean.csv not found. Please run data processing first.")
                    self.status_label.setStyleSheet("color: #7986cb; padding: 5px;")
                    self.df = None
                
                self.df_train = None
                self.df_val = None
                self.df_test = None
            
            self.update_overview()
            self.update_label_distribution()
            self.update_text_statistics()
            self.update_work_arrangement()
            
        except Exception as e:
            self.status_label.setText(f"Error loading data: {str(e)}")
            self.status_label.setStyleSheet("color: #7986cb; padding: 5px;")
    
    def create_overview_section(self):
        group = QGroupBox("Overview & Quality Gates")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        group.setLayout(layout)
        
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
        
        missing_label = QLabel("Missing Data by Column:")
        missing_label.setStyleSheet("font-weight: bold; color: #1a237e; margin-top: 10px;")
        layout.addWidget(missing_label)
        
        self.missing_canvas = FigureCanvas(Figure(figsize=(10, 4)))
        self.missing_canvas.setStyleSheet("background-color: white;")
        self.missing_canvas.setFixedHeight(400)
        layout.addWidget(self.missing_canvas)
        
        return group
    
    def create_label_distribution_section(self):
        group = QGroupBox("Label Distribution")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        layout.setSpacing(10)
        group.setLayout(layout)
        
        self.label_dist_canvas = FigureCanvas(Figure(figsize=(10, 5)))
        self.label_dist_canvas.setStyleSheet("background-color: white;")
        self.label_dist_canvas.setFixedHeight(500)
        layout.addWidget(self.label_dist_canvas)
        
        split_label = QLabel("Label Distribution by Split:")
        split_label.setStyleSheet("font-weight: bold; color: #1a237e; margin-top: 10px;")
        layout.addWidget(split_label)
        
        self.split_dist_canvas = FigureCanvas(Figure(figsize=(10, 5)))
        self.split_dist_canvas.setStyleSheet("background-color: white;")
        self.split_dist_canvas.setFixedHeight(500)
        layout.addWidget(self.split_dist_canvas)
        
        return group
    
    def create_text_statistics_section(self):
        group = QGroupBox("Text Statistics")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        layout.setSpacing(10)
        group.setLayout(layout)
        
        length_label = QLabel("Description Length Distribution:")
        length_label.setStyleSheet("font-weight: bold; color: #1a237e;")
        layout.addWidget(length_label)
        
        self.text_length_canvas = FigureCanvas(Figure(figsize=(10, 4)))
        self.text_length_canvas.setStyleSheet("background-color: white;")
        self.text_length_canvas.setFixedHeight(400)
        layout.addWidget(self.text_length_canvas)
        
        by_class_label = QLabel("Description Length by Experience Level:")
        by_class_label.setStyleSheet("font-weight: bold; color: #1a237e; margin-top: 10px;")
        layout.addWidget(by_class_label)
        
        self.text_by_class_canvas = FigureCanvas(Figure(figsize=(10, 5)))
        self.text_by_class_canvas.setStyleSheet("background-color: white;")
        self.text_by_class_canvas.setFixedHeight(500)
        layout.addWidget(self.text_by_class_canvas)
        
        return group
    
    def create_work_arrangement_section(self):
        group = QGroupBox("Work Arrangement & Compensation")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        layout.setSpacing(10)
        group.setLayout(layout)
        
        work_type_label = QLabel("Work Type Distribution:")
        work_type_label.setStyleSheet("font-weight: bold; color: #1a237e;")
        layout.addWidget(work_type_label)
        
        self.work_type_canvas = FigureCanvas(Figure(figsize=(10, 5)))
        self.work_type_canvas.setStyleSheet("background-color: white;")
        self.work_type_canvas.setFixedHeight(500)
        layout.addWidget(self.work_type_canvas)
        
        remote_label = QLabel("Remote Allowed Distribution:")
        remote_label.setStyleSheet("font-weight: bold; color: #1a237e; margin-top: 10px;")
        layout.addWidget(remote_label)
        
        self.remote_canvas = FigureCanvas(Figure(figsize=(8, 4)))
        self.remote_canvas.setStyleSheet("background-color: white;")
        self.remote_canvas.setFixedHeight(400)
        layout.addWidget(self.remote_canvas)
        
        salary_label = QLabel("Salary Distribution (Annual):")
        salary_label.setStyleSheet("font-weight: bold; color: #1a237e; margin-top: 10px;")
        layout.addWidget(salary_label)
        
        self.salary_canvas = FigureCanvas(Figure(figsize=(10, 5)))
        self.salary_canvas.setStyleSheet("background-color: white;")
        self.salary_canvas.setFixedHeight(500)
        layout.addWidget(self.salary_canvas)
        
        return group
    
    def update_overview(self):
        if self.df is None:
            return
        
        raw_rows = len(self.df)
        
        if self.dataset_type == "EN":
            rows_with_label = self.df["platform_experience_label"].notna().sum() if "platform_experience_label" in self.df.columns else 0
            rows_without_label = raw_rows - rows_with_label
            
            cov_years = self.df["years_hint"].notna().mean() * 100 if "years_hint" in self.df.columns else 0
            cov_title = self.df["title_hint"].notna().mean() * 100 if "title_hint" in self.df.columns else 0
            cov_platform = self.df["platform_experience_label"].notna().mean() * 100 if "platform_experience_label" in self.df.columns else 0
            
            self.metric_labels["Raw Rows"].setText(f"{raw_rows:,}")
            self.metric_labels["Rows with Platform Label"].setText(f"{rows_with_label:,}")
            self.metric_labels["Rows without Platform Label"].setText(f"{rows_without_label:,}")
            self.metric_labels["Years Hint Coverage"].setText(f"{cov_years:.1f}%")
            self.metric_labels["Title Hint Coverage"].setText(f"{cov_title:.1f}%")
            self.metric_labels["Platform Label Coverage"].setText(f"{cov_platform:.1f}%")
        else:
            rows_with_label = self.df["experience_label"].notna().sum() if "experience_label" in self.df.columns else 0
            rows_without_label = raw_rows - rows_with_label
            
            cov_years = self.df["years_hint"].notna().mean() * 100 if "years_hint" in self.df.columns else 0
            cov_title = self.df["title_hint"].notna().mean() * 100 if "title_hint" in self.df.columns else 0
            cov_experience = self.df["experience_label"].notna().mean() * 100 if "experience_label" in self.df.columns else 0
            cov_salary = self.df["salary_min"].notna().mean() * 100 if "salary_min" in self.df.columns else 0
            cov_remote = self.df["remote_allowed"].notna().mean() * 100 if "remote_allowed" in self.df.columns else 0
            pct_salary_suspect = (self.df["salary_suspect"].sum() / len(self.df) * 100) if "salary_suspect" in self.df.columns else 0
            
            self.metric_labels["Raw Rows"].setText(f"{raw_rows:,}")
            self.metric_labels["Rows with Platform Label"].setText(f"{rows_with_label:,}")
            self.metric_labels["Rows without Platform Label"].setText(f"{rows_without_label:,}")
            self.metric_labels["Years Hint Coverage"].setText(f"{cov_years:.1f}%")
            self.metric_labels["Title Hint Coverage"].setText(f"{cov_title:.1f}%")
            self.metric_labels["Platform Label Coverage"].setText(f"{cov_experience:.1f}%")
        
        if self.dataset_type == "EN":
            key_cols = ["title", "description_clean", "salary_annual_min", "salary_annual_max", 
                       "pay_period", "work_type", "remote_allowed", "location"]
        else:
            key_cols = ["title", "description_clean", "salary_min", "salary_max", 
                       "pay_period", "work_type", "remote_allowed", "location", 
                       "contract_type", "experience_label"]
        
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
        if self.df is None:
            return
        
        if self.dataset_type == "EN":
            label_col = "platform_experience_label"
        else:
            label_col = "experience_label"
        
        if label_col not in self.df.columns:
            return
        
        silver = self.df[self.df[label_col].notna()].copy()
        if len(silver) == 0:
            return
        
        label_counts = silver[label_col].value_counts()
        label_props = silver[label_col].value_counts(normalize=True) * 100
        
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
        
        fig = self.split_dist_canvas.figure
        fig.clear()
        ax = fig.add_subplot(111)
        
        if self.dataset_type == "EN" and self.df_train is not None and self.df_val is not None and self.df_test is not None:
            splits = {
                'Train': self.df_train,
                'Val': self.df_val,
                'Test': self.df_test
            }
            
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
            if self.dataset_type == "PL":
                ax.text(0.5, 0.5, 'PL dataset does not have train/val/test splits yet.', 
                       ha='center', va='center', transform=ax.transAxes,
                       fontsize=12, color='#5c6bc0')
                ax.set_title('Label Distribution by Split - Not Available')
            else:
                ax.text(0.5, 0.5, 'Split files not found.\nPlease run "Create Train/Val/Test Split" first.', 
                       ha='center', va='center', transform=ax.transAxes,
                       fontsize=12, color='#5c6bc0')
            ax.set_title('Label Distribution by Split - Files Not Found')
        
        fig.tight_layout()
        self.split_dist_canvas.draw()
    
    def update_text_statistics(self):
        if self.df is None or "platform_experience_label" not in self.df.columns:
            return
        
        silver = self.df[self.df["platform_experience_label"].notna()].copy()
        if len(silver) == 0:
            return
        
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
        if self.df is None:
            return
        
        silver = self.df[self.df["platform_experience_label"].notna()].copy() if "platform_experience_label" in self.df.columns else self.df.copy()
        
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
        
        if "salary_annual_min" in silver.columns and "salary_annual_max" in silver.columns:
            silver["salary_mid"] = np.where(
                silver["salary_annual_min"].notna() & silver["salary_annual_max"].notna(),
                (silver["salary_annual_min"] + silver["salary_annual_max"]) / 2,
                np.nan
            )
            salary_data = silver["salary_mid"].dropna()
            if len(salary_data) > 0:
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
    
class ModelPerformanceView(QWidget):
    
    def __init__(self):
        super().__init__()
        self.metrics_data = None
        self.predictions_df = None
        self.lang = "en"
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)
        
        lang_layout = QHBoxLayout()
        lang_label = QLabel("Language:")
        lang_label.setStyleSheet("font-weight: bold; color: #1a237e;")
        self.lang_combo = QComboBox()
        self.lang_combo.addItems(["EN", "PL"])
        self.lang_combo.setCurrentText("EN")
        self.lang_combo.currentTextChanged.connect(self.on_lang_changed)
        lang_layout.addWidget(lang_label)
        lang_layout.addWidget(self.lang_combo)
        lang_layout.addStretch()
        layout.addLayout(lang_layout)
        
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setStyleSheet("QScrollArea { border: none; }")
        
        content_widget = QWidget()
        content_layout = QVBoxLayout()
        content_layout.setSpacing(20)
        content_widget.setLayout(content_layout)
        
        content_layout.addWidget(self.create_overview_section())
        
        content_layout.addWidget(self.create_confusion_matrix_section())
        
        content_layout.addWidget(self.create_per_class_section())
        
        content_layout.addWidget(self.create_cv_metrics_section())
        
        content_layout.addWidget(self.create_error_table_section())
        
        content_layout.addStretch()
        scroll.setWidget(content_widget)
        layout.addWidget(scroll)
        
        self.setLayout(layout)
        
        self.load_data()
    
    def create_overview_section(self):
        group = QGroupBox("Model Performance Overview")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        group.setLayout(layout)
        
        info_layout = QGridLayout()
        self.info_labels = {}
        info_fields = ["Language", "Label Column", "Train Size", "Val Size", "Test Size", "Number of Classes"]
        
        for i, field in enumerate(info_fields):
            label = QLabel(f"{field}:")
            label.setStyleSheet("font-weight: bold; color: #1a237e;")
            value = QLabel("N/A")
            value.setStyleSheet("color: #5c6bc0; padding: 2px;")
            self.info_labels[field] = value
            
            row = i // 2
            col = (i % 2) * 2
            info_layout.addWidget(label, row, col)
            info_layout.addWidget(value, row, col + 1)
        
        layout.addLayout(info_layout)
        
        metrics_layout = QHBoxLayout()
        
        test_group = QGroupBox("Test Metrics")
        test_group.setStyleSheet("QGroupBox { font-weight: bold; }")
        test_layout = QVBoxLayout()
        self.test_labels = {}
        for metric in ["Accuracy", "F1 Macro", "F1 Weighted"]:
            label = QLabel(f"{metric}:")
            label.setStyleSheet("font-weight: bold; color: #1a237e;")
            value = QLabel("N/A")
            value.setStyleSheet("color: #3949ab; font-size: 14px; padding: 5px;")
            self.test_labels[metric] = value
            test_layout.addWidget(label)
            test_layout.addWidget(value)
        test_group.setLayout(test_layout)
        metrics_layout.addWidget(test_group)
        
        val_group = QGroupBox("Validation Metrics")
        val_group.setStyleSheet("QGroupBox { font-weight: bold; }")
        val_layout = QVBoxLayout()
        self.val_labels = {}
        for metric in ["Accuracy", "F1 Macro", "F1 Weighted"]:
            label = QLabel(f"{metric}:")
            label.setStyleSheet("font-weight: bold; color: #1a237e;")
            value = QLabel("N/A")
            value.setStyleSheet("color: #3949ab; font-size: 14px; padding: 5px;")
            self.val_labels[metric] = value
            val_layout.addWidget(label)
            val_layout.addWidget(value)
        val_group.setLayout(val_layout)
        metrics_layout.addWidget(val_group)
        
        layout.addLayout(metrics_layout)
        
        return group
    
    def create_confusion_matrix_section(self):
        group = QGroupBox("Confusion Matrix")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        group.setLayout(layout)
        
        self.confusion_canvas = FigureCanvas(Figure(figsize=(8, 7)))
        self.confusion_canvas.setStyleSheet("background-color: white;")
        self.confusion_canvas.setFixedHeight(500)
        layout.addWidget(self.confusion_canvas)
        
        return group
    
    def create_per_class_section(self):
        group = QGroupBox("Per-Class Metrics")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        group.setLayout(layout)
        
        # F1 per class
        f1_label = QLabel("F1 Score by Class:")
        f1_label.setStyleSheet("font-weight: bold; color: #1a237e;")
        layout.addWidget(f1_label)
        
        self.f1_canvas = FigureCanvas(Figure(figsize=(10, 5)))
        self.f1_canvas.setStyleSheet("background-color: white;")
        self.f1_canvas.setFixedHeight(400)
        layout.addWidget(self.f1_canvas)
        
        recall_label = QLabel("Recall by Class:")
        recall_label.setStyleSheet("font-weight: bold; color: #1a237e; margin-top: 10px;")
        layout.addWidget(recall_label)
        
        self.recall_canvas = FigureCanvas(Figure(figsize=(10, 5)))
        self.recall_canvas.setStyleSheet("background-color: white;")
        self.recall_canvas.setFixedHeight(400)
        layout.addWidget(self.recall_canvas)
        
        return group
    
    def create_cv_metrics_section(self):
        group = QGroupBox("Cross-Validation Metrics (Stability)")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        group.setLayout(layout)
        
        self.cv_labels = {}
        cv_layout = QGridLayout()
        cv_fields = ["CV Accuracy", "CV F1 Macro", "CV F1 Weighted"]
        
        for i, field in enumerate(cv_fields):
            label = QLabel(f"{field}:")
            label.setStyleSheet("font-weight: bold; color: #1a237e;")
            value = QLabel("N/A")
            value.setStyleSheet("color: #5c6bc0; padding: 2px;")
            self.cv_labels[field] = value
            
            row = i // 2
            col = (i % 2) * 2
            cv_layout.addWidget(label, row, col)
            cv_layout.addWidget(value, row, col + 1)
        
        layout.addLayout(cv_layout)
        
        stability_label = QLabel("Stability (lower std = more stable):")
        stability_label.setStyleSheet("font-weight: bold; color: #1a237e; margin-top: 10px;")
        layout.addWidget(stability_label)
        
        self.stability_labels = {}
        stability_layout = QGridLayout()
        stability_fields = ["F1 Macro Std", "Accuracy Std"]
        
        for i, field in enumerate(stability_fields):
            label = QLabel(f"{field}:")
            label.setStyleSheet("font-weight: bold; color: #1a237e;")
            value = QLabel("N/A")
            value.setStyleSheet("color: #5c6bc0; padding: 2px;")
            self.stability_labels[field] = value
            
            row = i // 2
            col = (i % 2) * 2
            stability_layout.addWidget(label, row, col)
            stability_layout.addWidget(value, row, col + 1)
        
        layout.addLayout(stability_layout)
        
        return group
    
    def create_error_table_section(self):
        group = QGroupBox("Prediction Errors (Test Set)")
        group.setFont(QFont("Arial", 12, QFont.Bold))
        layout = QVBoxLayout()
        group.setLayout(layout)
        
        filter_layout = QHBoxLayout()
        filter_label = QLabel("Show:")
        filter_label.setStyleSheet("font-weight: bold; color: #1a237e;")
        self.error_filter_combo = QComboBox()
        self.error_filter_combo.addItems(["All", "Errors Only", "Correct Only"])
        self.error_filter_combo.currentTextChanged.connect(self.update_error_table)
        filter_layout.addWidget(filter_label)
        filter_layout.addWidget(self.error_filter_combo)
        filter_layout.addStretch()
        layout.addLayout(filter_layout)
        
        self.error_table = QTextEdit()
        self.error_table.setReadOnly(True)
        self.error_table.setStyleSheet("background-color: white; font-family: monospace;")
        self.error_table.setFixedHeight(300)
        layout.addWidget(self.error_table)
        
        return group
    
    def on_lang_changed(self, lang_text):
        self.lang = lang_text.lower()
        self.load_data()
    
    def load_data(self):
        metrics_path = Path(f"model_output/metrics_{self.lang}.json")
        if self.lang == "en":
            pred_path = Path("model_output/baseline_test_predictions.csv")
        else:
            pred_path = Path(f"model_output/baseline_test_predictions_{self.lang}.csv")
        
        if metrics_path.exists():
            try:
                with open(metrics_path, 'r', encoding='utf-8') as f:
                    self.metrics_data = json.load(f)
            except Exception as e:
                print(f"Error loading metrics: {e}")
                self.metrics_data = None
        else:
            self.metrics_data = None
        
        if pred_path.exists():
            try:
                self.predictions_df = pd.read_csv(pred_path, low_memory=False)
            except Exception as e:
                print(f"Error loading predictions: {e}")
                self.predictions_df = None
        else:
            self.predictions_df = None
        
        self.update_overview()
        self.update_confusion_matrix()
        self.update_per_class_metrics()
        self.update_cv_metrics()
        self.update_error_table()
    
    def update_overview(self):
        if self.metrics_data is None:
            return
        
        self.info_labels["Language"].setText(self.metrics_data.get("lang", "N/A"))
        self.info_labels["Label Column"].setText(self.metrics_data.get("label_col", "N/A"))
        self.info_labels["Train Size"].setText(str(self.metrics_data.get("n_train", "N/A")))
        self.info_labels["Val Size"].setText(str(self.metrics_data.get("n_val", "N/A")))
        self.info_labels["Test Size"].setText(str(self.metrics_data.get("n_test", "N/A")))
        self.info_labels["Number of Classes"].setText(str(self.metrics_data.get("n_classes", "N/A")))
        
        test_metrics = self.metrics_data.get("test_metrics", {})
        self.test_labels["Accuracy"].setText(f"{test_metrics.get('accuracy', 0):.3f}")
        self.test_labels["F1 Macro"].setText(f"{test_metrics.get('f1_macro', 0):.3f}")
        self.test_labels["F1 Weighted"].setText(f"{test_metrics.get('f1_weighted', 0):.3f}")
        
        val_metrics = self.metrics_data.get("val_metrics", {})
        self.val_labels["Accuracy"].setText(f"{val_metrics.get('accuracy', 0):.3f}")
        self.val_labels["F1 Macro"].setText(f"{val_metrics.get('f1_macro', 0):.3f}")
        self.val_labels["F1 Weighted"].setText(f"{val_metrics.get('f1_weighted', 0):.3f}")
    
    def update_confusion_matrix(self):
        if self.metrics_data is None:
            return
        
        cm = self.metrics_data.get("confusion_matrix")
        labels = self.metrics_data.get("confusion_labels", [])
        
        if cm is None or len(labels) == 0:
            return
        
        fig = self.confusion_canvas.figure
        fig.clear()
        ax = fig.add_subplot(111)
        
        cm_array = np.array(cm)
        
        if HAS_SEABORN:
            sns.heatmap(cm_array, annot=True, fmt='d', cmap='Blues', ax=ax,
                       xticklabels=labels, yticklabels=labels,
                       cbar_kws={'label': 'Count'})
        else:
            im = ax.imshow(cm_array, cmap='Blues', aspect='auto')
            ax.set_xticks(range(len(labels)))
            ax.set_yticks(range(len(labels)))
            ax.set_xticklabels(labels)
            ax.set_yticklabels(labels)
            
            for i in range(len(labels)):
                for j in range(len(labels)):
                    text = ax.text(j, i, int(cm_array[i, j]),
                                 ha="center", va="center", color="black", fontweight='bold')
            
            fig.colorbar(im, ax=ax, label='Count')
        
        ax.set_xlabel('Predicted', fontsize=12)
        ax.set_ylabel('True', fontsize=12)
        ax.set_title('Confusion Matrix', fontsize=14, fontweight='bold')
        ax.tick_params(axis='x', rotation=45)
        ax.tick_params(axis='y', rotation=0)
        
        fig.tight_layout()
        self.confusion_canvas.draw()
    
    def update_per_class_metrics(self):
        if self.metrics_data is None:
            return
        
        per_class = self.metrics_data.get("per_class_metrics", {})
        if not per_class:
            return
        
        classes = list(per_class.keys())
        f1_scores = [per_class[c].get("f1", 0) for c in classes]
        recalls = [per_class[c].get("recall", 0) for c in classes]
        supports = [per_class[c].get("support", 0) for c in classes]
        
        fig = self.f1_canvas.figure
        fig.clear()
        ax = fig.add_subplot(111)
        
        bars = ax.bar(classes, f1_scores, color='#5c6bc0', edgecolor='#3949ab')
        ax.set_ylabel('F1 Score', fontsize=11)
        ax.set_xlabel('Class', fontsize=11)
        ax.set_title('F1 Score by Class', fontsize=12, fontweight='bold')
        ax.set_ylim(0, 1.0)
        ax.tick_params(axis='x', rotation=45)
        
        # Add support annotations
        for i, (bar, support) in enumerate(zip(bars, supports)):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                   f'n={support}', ha='center', va='bottom', fontsize=9)
        
        fig.tight_layout()
        self.f1_canvas.draw()
        
        fig = self.recall_canvas.figure
        fig.clear()
        ax = fig.add_subplot(111)
        
        bars = ax.bar(classes, recalls, color='#7986cb', edgecolor='#5c6bc0')
        ax.set_ylabel('Recall', fontsize=11)
        ax.set_xlabel('Class', fontsize=11)
        ax.set_title('Recall by Class', fontsize=12, fontweight='bold')
        ax.set_ylim(0, 1.0)
        ax.tick_params(axis='x', rotation=45)
        
        # Add support annotations
        for i, (bar, support) in enumerate(zip(bars, supports)):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                   f'n={support}', ha='center', va='bottom', fontsize=9)
        
        fig.tight_layout()
        self.recall_canvas.draw()
    
    def update_cv_metrics(self):
        if self.metrics_data is None:
            return
        
        cv_metrics = self.metrics_data.get("cv_metrics")
        if cv_metrics is None:
            return
        
        self.cv_labels["CV Accuracy"].setText(
            f"{cv_metrics.get('cv_accuracy_mean', 0):.3f} ± {cv_metrics.get('cv_accuracy_std', 0):.3f}"
        )
        self.cv_labels["CV F1 Macro"].setText(
            f"{cv_metrics.get('cv_f1_macro_mean', 0):.3f} ± {cv_metrics.get('cv_f1_macro_std', 0):.3f}"
        )
        self.cv_labels["CV F1 Weighted"].setText(
            f"{cv_metrics.get('cv_f1_weighted_mean', 0):.3f} ± {cv_metrics.get('cv_f1_weighted_std', 0):.3f}"
        )
        
        stability = self.metrics_data.get("stability", {})
        self.stability_labels["F1 Macro Std"].setText(f"{stability.get('f1_macro_std', 0):.4f}")
        self.stability_labels["Accuracy Std"].setText(f"{stability.get('accuracy_std', 0):.4f}")
    
    def update_error_table(self):
        if self.predictions_df is None:
            self.error_table.setText("No predictions data available.")
            return
        
        filter_text = self.error_filter_combo.currentText()
        
        if filter_text == "Errors Only":
            df = self.predictions_df[~self.predictions_df["correct"]].copy()
        elif filter_text == "Correct Only":
            df = self.predictions_df[self.predictions_df["correct"]].copy()
        else:
            df = self.predictions_df.copy()
        
        if len(df) == 0:
            self.error_table.setText(f"No {filter_text.lower()} predictions found.")
            return
        
        cols = ["job_id", "title", "true_label", "predicted_label", "correct"]
        available_cols = [c for c in cols if c in df.columns]
        
        header = " | ".join([col.ljust(20) for col in available_cols])
        separator = "-" * len(header)
        lines = [header, separator]
        
        for _, row in df.head(50).iterrows():
            line = " | ".join([str(row.get(col, "")).ljust(20)[:20] for col in available_cols])
            lines.append(line)
        
        if len(df) > 50:
            lines.append(f"\n... and {len(df) - 50} more rows")
        
        self.error_table.setText("\n".join(lines))


class MainWindow(QMainWindow):
    
    def __init__(self):
        super().__init__()
        self.init_ui()
    
    def init_ui(self):
        self.setWindowTitle("Job Posting Analysis Pipeline")
        self.setGeometry(100, 100, 1200, 800)
        
        navbar = self.create_navbar()
        self.addToolBar(Qt.TopToolBarArea, navbar)
        
        self.stack = QStackedWidget()
        
        self.main_view = MainView()
        self.stack.addWidget(self.main_view)
        
        self.try_it_view = TryItOutView()
        self.stack.addWidget(self.try_it_view)
        
        self.stats_view = DatasetStatisticsView()
        self.stack.addWidget(self.stats_view)
        
        self.setCentralWidget(self.stack)
    
    def create_navbar(self):
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
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
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
