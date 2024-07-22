import asyncio
import collections
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from genericpath import isdir
import glob
import io
from itertools import zip_longest
from operator import contains
import os
from pathlib import Path
import pickle
import envyaml
import re
from statistics import median
from typing import cast
import tempdir
import time
import timeit
from tkinter import Menu
import flet as ft

import numpy as np
import pyautogui
import s3fs
import pydicom.filereader
from scandir import walk
import pandas as pd
import pydicom
import pydicom.dataelem
import filetype

from datasetwalker.backend.utils import get_human_readable_size


NEEDED_DICOM_TAGS_FOR_REPORT = [
    "PatientID",
    "PatientAge",
    "PatientSex",
    "StudyInstanceUID",
    "StudyDescription",
    "SeriesInstanceUID",
    "Modality",
    "SliceThickness",
    "ConvolutionKernel",
    "SeriesDescription",
    "Manufacturer",
    "ManufacturerModelName",
    "DeviceID",
    "DeviceSerialNumber",
]
DICOM_EXT = "dcm"


def get_config():
    return envyaml.EnvYAML(
        "config.yml", ".env", include_environment=False, flatten=False
    )


class FileSystem:
    def list_files_recursively(self):
        raise NotImplementedError

    def file_exists(self):
        raise NotImplementedError

    def is_dir(self):
        raise NotImplementedError

    def is_file(self):
        raise NotImplementedError

    def change_dir(self):
        raise NotImplementedError

    def rm_file(self):
        raise NotImplementedError

    def create_file(self):
        raise NotImplementedError

    def rm_dir(self):
        raise NotImplementedError

    def create_dir(self):
        raise NotImplementedError

    def get_fileobj(self):
        raise NotImplementedError


class LocalFileSystem(FileSystem):
    def list_files_recursively(
        self, path: str | Path, extension: str = DICOM_EXT
    ) -> list[str]:
        path = Path(str(path))

        if not path.exists():
            raise ValueError(path)

        if extension == DICOM_EXT:
            return [
                f
                for f in path.glob("**/*")
                if f.is_file()
                for ftype in [filetype.guess(f)]
                if (hasattr(ftype, "extension") and ftype.extension == "dcm")
            ]

        files = []

        def scan_dirs(obj):
            nonlocal files
            root, _, fns = obj

            for fn in fns:
                try:
                    path = os.path.join(root, fn)
                    if self.is_has_ext(path, extension):
                        files.append(path)

                except Exception:
                    return

        with ThreadPoolExecutor() as executor:
            executor.map(
                scan_dirs,
                walk(str(path)),
            )

        return files

    def size_in_bytes(self, path: str | Path):
        f = Path(path)
        return f.stat().st_size

    def glob(self, path, pattern):
        return Path(path).glob(pattern)

    @property
    def default_path(self):
        return os.getcwd()

    def exists(self, path: str):
        return Path(path).exists()

    def is_dir(self, path: str):
        return Path(path).is_dir()

    def is_file(self, path: str):
        return Path(path).is_file()

    def is_has_ext(self, path: str, ext: str = DICOM_EXT):
        file = filetype.guess(path)
        if hasattr(file, "extension") and file.extension == ext:
            return True
        return False

    def raw_size_of_file(self, path):
        return Path(path).stat().st_size

    def get_size_of_dir(self, path):
        return len(os.listdir(path))

    def list_files_in_dir(self, path):
        return self.glob(path, "*")

    def change_dir(self): ...

    def rm_file(self): ...

    def create_file(self): ...

    def rm_dir(self): ...

    def create_dir(self): ...

    def get_fileobj(self, path: str | Path):
        with open(path, "rb") as f:
            content_bytes = f.read()
        return io.BytesIO(content_bytes)

    def absolute_path(self, path) -> Path:
        return Path(path).absolute()

    def parrent_dir(self, path):
        return Path(path).parent.absolute()


def hr_size(raw_bytes_size):
    n = ["Bytes", "KB", "MB", "GB", "TB"]
    size_n = n.pop(0)

    while True:
        if raw_bytes_size > 1024:
            raw_bytes_size /= 1024
            size_n = n.pop(0)
            continue
        return round(raw_bytes_size, 2), size_n


cfg = get_config()


class S3FileSystem(LocalFileSystem):
    _AWS_ACCESS_KEY_ID = cfg["aws"]["access_key"]
    _AWS_SECRET_ACCESS_KEY = cfg["aws"]["secret_key"]
    _ENDPOINT_URL = cfg["aws"]["endpoint_url"]

    def __init__(self) -> None:
        super().__init__()
        self._fs = s3fs.S3FileSystem(
            endpoint_url=self._ENDPOINT_URL,
            key=self._AWS_ACCESS_KEY_ID,
            secret=self._AWS_SECRET_ACCESS_KEY,
        )
        self._container = "Heap"

    def list_files_recursively(
        self, path: str | Path = None, extension: str = DICOM_EXT
    ) -> list[str]:
        if not path:
            path = self._container
        else:
            path = str(path)

        files = []

        def scan_dirs(obj):
            nonlocal files
            root, _, fns = obj

            for fn in fns:
                if fn.endswith("/") or not fn:
                    continue

                rpath = os.path.join(root, fn)

                try:
                    if self.is_has_ext(rpath, extension):
                        files.append(str(rpath))

                except Exception as e:
                    print(e.__class__.__name__, str(e))
                    continue

        with ThreadPoolExecutor() as executor:
            executor.map(
                scan_dirs,
                self._fs.walk(path),
            )

        return files

    @property
    def default_path(self):
        return self._container

    def size_in_bytes(self, path: str | Path):
        try:
            return int(self._fs.size(path))
        except (TypeError, AttributeError):
            return None

    def glob(self, path, pattern):
        res = self._fs.glob(f"{path}/{pattern}")
        return res

    def is_has_ext(self, path: str, ext: str = DICOM_EXT):
        content = self._fs.read_block(path, 0, length=8192)
        file = filetype.guess(content)
        if hasattr(file, "extension") and file.extension == ext:
            return True
        return False

    def exists(self, path: str):
        return self._fs.exists(path)

    def is_dir(self, path: str):
        return self._fs.isdir(path)

    def is_file(self, path: str):
        return self._fs.isfile(path)

    def raw_size_of_file(self, path):
        return self._fs.size(path)

    def get_size_of_dir(self, path):
        return len(self._fs.listdir(path))

    def change_dir(self): ...

    def rm_file(self): ...

    def create_file(self): ...

    def rm_dir(self): ...

    def create_dir(self): ...

    def get_fileobj(self, path: str | Path):
        content_bytes = self._fs.read_bytes(path)
        return io.BytesIO(content_bytes)

    def parrent_dir(self, path: str):
        path = str(path)
        sep = self._fs.sep
        if path.endswith(sep):
            index = path.rfind(sep)
            path = path[:index]
        return sep.join(path.split(sep)[:-1])


def get_border_color(page: ft.Page):
    if page.theme_mode == ft.ThemeMode.LIGHT:
        return ft.colors.BLACK
    if page.theme_mode == ft.ThemeMode.DARK:
        return ft.colors.WHITE
    return ft.colors.WHITE


class CustomExplorer(ft.Container):
    columns = ["Name", "Size"]
    padding_left_right = 40
    icon_size = 30

    def __init__(self, page: ft.Page):
        super().__init__(
            on_click=self.close_dropdown_path,
            expand=True,
            alignment=ft.alignment.center,
        )

        self._filesystem = LocalFileSystem()
        self._nesting_level = 1

        self.storage_type = "local"
        self.storage_types = ["local", "s3"]

        self._current_path = "/home/daniil/VSCode"
        self._paths = ["/home/daniil/VSCode"]
        self._paths = [
            "/home/daniil/VSCode/hiveomics/DatasetWalker/dataset"
            "/ishemia_21022022"
        ]
        self._hidden_files = False

        self.search_bar = ft.Ref[ft.TextField]()
        search_bar = ft.Container(
            content=ft.Row(
                controls=[
                    ft.Container(
                        ft.Icon(
                            ft.icons.ARROW_BACK,
                            size=25,
                        ),
                        on_click=self.go_back,
                    ),
                    ft.TextField(
                        label="Path",
                        label_style=ft.TextStyle(
                            size=15,
                            weight=ft.FontWeight.BOLD,
                        ),
                        # value=os.path.split(str(self._current_path))[1],
                        value=os.path.split(str(self._paths[0]))[1],
                        text_style=ft.TextStyle(
                            size=15,
                            weight=ft.FontWeight.BOLD,
                        ),
                        expand=True,
                        on_focus=self.on_focus_searchbar,
                        on_change=self.show_dropdown_with_paths,
                        on_submit=self.go_to_path,
                        ref=self.search_bar,
                    ),
                    ft.Checkbox(
                        "Show Hidden Files",
                        self._hidden_files,
                        on_change=self.show_hidden_files,
                    ),
                ],
                expand=True,
            ),
            border_radius=ft.border_radius.all(8),
            padding=ft.padding.all(10),
        )

        def set_color(e):
            color = ft.colors.with_opacity(0.4, ft.colors.BLUE_300)
            e.control.bgcolor = color if e.data == "true" else None
            self.page.update()

        columns_data = ft.Row(
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            controls=[
                ft.Container(
                    ft.Row(
                        [
                            ft.Text(n, size=17, weight=ft.FontWeight.W_700),
                            ft.Icon(
                                ft.icons.KEYBOARD_ARROW_DOWN_SHARP,
                                size=30,
                                visible=False,
                            ),
                        ],
                    ),
                    on_click=self.filter_files,
                    on_hover=set_color,
                    padding=ft.padding.only(
                        left=40, right=40, top=10, bottom=10
                    ),
                )
                for n in self.columns
            ],
        )
        columns_data.controls[-1].content.alignment = ft.MainAxisAlignment.END
        # rows_data = self.get_files(self._current_path)
        rows_data = self.get_files(self._paths[0])

        def on_tap(e: ft.TapEvent):
            gesture_detector: ft.GestureDetector = table.content.controls[1]

            column: ft.Stack = gesture_detector.content

            context_menu.visible = True
            context_menu.top = e.local_y + 20
            context_menu.left = e.local_x + 25

            def hide_menu(e):
                if context_menu.visible:
                    context_menu.visible = False
                    self.page.update()

            gesture_detector.on_tap = hide_menu
            column.controls[-1] = context_menu
            self.content.controls[0].controls[1] = table
            self.page.update()

        context_menu = ft.Container(
            ft.Column(
                controls=[
                    ft.Text("show hidden files"),
                ],
            ),
            visible=False,
            border=ft.border.all(1),
            padding=ft.padding.all(10),
            border_radius=ft.border_radius.all(6),
            bgcolor=get_border_color(page),
        )

        self.files_container = ft.Ref[ft.Container]()
        self.explorer_window = ft.Ref[ft.Container]()

        table = ft.Container(
            ft.Column(
                controls=[
                    ft.Container(
                        columns_data,
                        border=ft.border.only(bottom=ft.BorderSide(width=2)),
                    ),
                    ft.GestureDetector(
                        content=ft.Stack(
                            controls=[
                                ft.Container(
                                    ft.ListView(
                                        controls=rows_data,
                                        height=500,
                                        spacing=15,
                                    ),
                                    padding=ft.padding.only(bottom=0, top=0),
                                    expand=True,
                                    ref=self.files_container,
                                ),
                                context_menu,
                            ],
                        ),
                        on_secondary_tap_up=on_tap,
                    ),
                ],
            ),
            ref=self.explorer_window,
            border=ft.border.all(2),
        )

        border_color = get_border_color(page)
        self.dropdown = ft.Ref[ft.Container]()
        dropdown = ft.Container(
            border=ft.border.all(1, border_color),
            border_radius=ft.border_radius.all(4),
            content=ft.Column(
                spacing=0,
                controls=[],
                scroll=ft.ScrollMode.HIDDEN,
            ),
            top=60,
            left=60,
            on_click=lambda x: "any",
            visible=False,
            bgcolor=ft.colors.BLUE_GREY_800,
            ref=self.dropdown,
        )
        self.export = ft.Container(
            ft.Row(
                controls=[
                    ft.FilledTonalButton(
                        content=ft.Text("Export"),
                        style=ft.ButtonStyle(bgcolor="red", color="white"),
                        on_click=self.on_click_export,
                    )
                ],
                alignment=ft.MainAxisAlignment.CENTER,
            ),
            bottom=15,
            right=150,
            offset=ft.Offset(3, 0),
            animate_offset=ft.animation.Animation(
                1200, ft.AnimationCurve.EASE_IN_OUT_QUART
            ),
        )

        self.content = ft.Stack(
            controls=[
                ft.Column(
                    [
                        search_bar,
                        ft.Stack(
                            controls=[table, self.export],
                            alignment=ft.alignment.bottom_center,
                        ),
                    ]
                ),
                dropdown,
            ],
            alignment=ft.alignment.center,
        )

    def _get_sorted_filters_as_controls(self) -> ft.Row:
        return self.explorer_window.current.content.controls[0].content

    def _get_enabled_sort_filter(self):
        for control in self._get_sorted_filters_as_controls().controls:
            control: ft.Container
            if control.content.controls[1].visible:
                return control

    def _off_another_filters(self, target_filter_name: str):
        for sort_filter in self._get_sorted_filters_as_controls().controls:
            sort_filter: ft.Container

            icon = sort_filter.content.controls[1]
            filter_name = sort_filter.content.controls[0].value

            if icon.visible and filter_name.lower() != target_filter_name:
                sort_filter.bgcolor = None
                icon.visible = False

    def _sort_files_by(self, filter_name: str, reverse: bool):
        files = self.files_container.current.content.controls.copy()
        if filter_name == "name":
            files = sorted(
                files,
                key=lambda f: f.content.controls[0].content.controls[-1].value,
                reverse=reverse,
            )
            return files

        elif filter_name == "size":
            dirs = [
                i
                for i in files
                if i.content.controls[0].on_click == self.on_dir_clicked
            ]
            files_ = [i for i in files if i not in dirs]

            sorted_dirs = sorted(
                dirs, key=lambda d: d.raw_size, reverse=reverse
            )

            sorted_files = sorted(
                files_, key=lambda f: f.raw_size, reverse=reverse
            )

            files = sorted_dirs + sorted_files
            return files

    def filter_files(self, e):
        icon = e.control.content.controls[1]
        column: str = e.control.content.controls[0].value.lower()
        self._off_another_filters(column)

        if icon.name == ft.icons.KEYBOARD_ARROW_DOWN_SHARP:
            icon = ft.icons.KEYBOARD_ARROW_UP_SHARP
            reverse = False
        else:
            icon = ft.icons.KEYBOARD_ARROW_DOWN_SHARP
            reverse = True

        self.files_container.current.content.controls = self._sort_files_by(
            filter_name=column, reverse=reverse
        )

        e.control.content.controls[1] = ft.Icon(
            icon,
            size=30,
        )
        self.page.update()

    @property
    def fs(self):
        return self._filesystem

    def _to_statistic_from_df(self, dataset: dict) -> pd.DataFrame:
        lengths = [len(dataset[column_n]) for column_n in dataset]
        if not all(lengths[0] == lenth for lenth in lengths):
            msg = (
                "The dataset is incorrect! The keys must be "
                "column names, and the values must be a list of "
                "values. Each list should be the same length as "
                f"the others. The length of columns dataset: {lengths}"
            )
            raise ValueError(msg)

        def list_without_null(src: list):
            return list(map(lambda x: x if x else "", src))

        _df = pd.DataFrame(dataset)
        _df["PatientAge"] = (
            _df["PatientAge"].str.extract(r"(\d+)", expand=False).astype(int)
        )

        patients = list_without_null(dataset["PatientID"])

        mens = _df[_df["PatientSex"] == "M"]
        womans = _df[_df["PatientSex"] == "F"]
        other = _df[_df["PatientSex"] == ""]

        max_age = _df["PatientAge"].max()
        min_age = _df["PatientAge"].min()
        med_age = _df["PatientAge"].median()

        patients_data = [
            ["Patients", "Total", _df["PatientID"].nunique()],
            ["Patients", "Male", mens["PatientID"].nunique()],
            ["Patients", "Female", womans["PatientID"].nunique()],
            ["Patients", "Other", other["PatientID"].nunique()],
            ["Patients", "MaxAge", max_age],
            ["Patients", "MinAge", min_age],
            ["Patients", "MedianAge", med_age],
        ]

        count_per_pacient = _df.groupby("PatientID")[
            "StudyInstanceUID"
        ].nunique()

        studies_data = [
            ["Studies", "Total", count_per_pacient.count()],
            ["Studies", "MaxPerPacient", count_per_pacient.max()],
            ["Studies", "MinPerPacient", count_per_pacient.min()],
            ["Studies", "MedianPerPacient", count_per_pacient.median()],
        ]

        study_descriptions = _df.groupby("StudyDescription")["StudyInstanceUID"]
        study_description_data = [
            [
                "StudyDescriptions",
                "TotalDistinct",
                study_descriptions.unique().count(),
            ],
        ]

        study_description_data.extend(
            [
                [
                    "StudyDescriptions",
                    description,
                    value,
                ]
                for (description, value) in study_descriptions.nunique().items()
            ]
        )

        per_study = _df.groupby("StudyInstanceUID")[
            "SeriesInstanceUID"
        ].nunique()
        series_data = [
            ["Series", "Total", per_study.sum()],
            ["Series", "MinPerStudy", per_study.min()],
            ["Series", "MaxPerStudy", per_study.max()],
            ["Series", "MedianPerStudy", per_study.median()],
        ]

        series_modalites = _df.groupby("Modality")[
            "SeriesInstanceUID"
        ].nunique()
        series_modalites_data = [
            ["SeriesModalities", "TotalDistinct", series_modalites.count()],
        ]
        series_modalites_data.extend(
            [
                [
                    "SeriesModalities",
                    modality,
                    count,
                ]
                for modality, count in series_modalites.items()
            ]
        )

        slices_thickness = _df.groupby("SliceThickness")[
            "SeriesInstanceUID"
        ].nunique()
        slices_thickness_data = [
            ["Slice Thicknesses", "TotalDistinct", slices_thickness.count()],
        ]
        slices_thickness_data.extend(
            [
                [
                    "Slice Thicknesses",
                    slice_thck,
                    count,
                ]
                for slice_thck, count in slices_thickness.items()
            ]
        )

        series_descriptions = _df.groupby("SeriesDescription")[
            "StudyInstanceUID"
        ].nunique()
        series_descriptions_data = [
            [
                "SeriesDescriptions",
                "TotalDistinct",
                series_descriptions.count(),
            ],
        ]
        series_descriptions_data.extend(
            [
                [
                    "SeriesDescriptions",
                    description,
                    count,
                ]
                for description, count in series_descriptions.items()
            ]
        )

        devices = _df.groupby("DeviceID")["StudyInstanceUID"].nunique()
        devices_data = [
            ["Devices", "TotalDistinct", devices.count()],
        ]
        devices_data.extend(
            [["DeviceIDs", device, count] for device, count in devices.items()]
        )

        device_manufacturures = _df.groupby("Manufacturer")[
            "StudyInstanceUID"
        ].nunique()
        device_manufacturures_data = [
            [
                "DevicesManufacturers",
                manufacturer,
                count,
            ]
            for manufacturer, count in device_manufacturures.items()
        ]

        device_models = _df.groupby("ManufacturerModelName")[
            "StudyInstanceUID"
        ].nunique()
        device_models_data = [
            [
                "DevicesModels",
                manufacturer,
                count,
            ]
            for manufacturer, count in device_models.items()
        ]

        convolution_kernels = _df.groupby("ConvolutionKernel")[
            "StudyInstanceUID"
        ].nunique()
        convolution_kernels_data = [
            [
                "Convolution Kernels",
                "TotalDistinct",
                convolution_kernels.count(),
            ]
        ]
        convolution_kernels_data.extend(
            [
                [
                    "Convolution Kernels",
                    kernel,
                    count,
                ]
                for kernel, count in convolution_kernels.items()
            ]
        )

        data_sets = [(k, v) for k, v in locals().items() if k.endswith("_data")]
        data_df = {"Group": [], "Detail": [], "Value": []}
        for _, data in data_sets:
            for row in data:
                group, detail, value = row
                data_df["Group"].append(group)
                data_df["Detail"].append(detail)
                data_df["Value"].append(value)

        df = pd.DataFrame(data_df)
        return df

    def _get_dataset(self, path: str):
        data = {"Group": [], "Detail": [], "Value": []}
        # files = pickle.load(open("files.pkl", "rb"))
        start = time.time()
        fileobjs = (
            self.fs.get_fileobj(path)
            for path in self.fs.list_files_recursively(path)
        )
        print(time.time() - start)

        columns = NEEDED_DICOM_TAGS_FOR_REPORT
        data = {k: [] for k in columns}

        def scan_file(obj):
            nonlocal data
            ds = pydicom.dcmread(obj)

            for column in columns:
                if hasattr(ds, column):
                    data[column].append(getattr(ds, column))
                else:
                    data[column].append(None)
                    # if column in [
                    #     "DeviceID",
                    #     "DeviceSerialNumber",
                    #     "SliceThickness",
                    #     "ConvolutionKernel",
                    #     "PatientAge",
                    # ]:
                    #     data[column].append(None)
                    # else:
                    #     msg = f"Uknown tag in dcm file: {column}"
                    #     raise ValueError(msg)

        with ThreadPoolExecutor() as executor:
            start = time.time()
            executor.map(scan_file, fileobjs)
        print(time.time() - start)

        # with open     ("dataset_data_fileobjs.pkl", "wb") as f:
        #     pickle.dump(data, f)

        # data = pickle.load(open("dataset_data.pkl", "rb"))
        # data = pickle.load(open("dataset_data_fileobjs.pkl", "rb"))

        return self._to_statistic_from_df(data)

    def _get_export_window(self):
        start = time.time()
        # df = self._get_dataset(self._current_path)
        df = self._get_dataset(self._paths[0])
        print(time.time() - start)

        modal_content = ft.Column(
            controls=[
                ft.Container(
                    ft.Row(
                        controls=[
                            ft.Column(
                                controls=[
                                    ft.Text(name[1].iloc[0])
                                    for name in df.iterrows()
                                ]
                            ),
                            ft.Column(
                                controls=[
                                    ft.Text(name[1].iloc[1])
                                    for name in df.iterrows()
                                ]
                            ),
                            ft.Column(
                                controls=[
                                    ft.Text(name[1].iloc[2])
                                    for name in df.iterrows()
                                ]
                            ),
                        ]
                    ),
                    padding=ft.padding.only(left=20, right=20),
                ),
            ],
            height=400,
            alignment=ft.MainAxisAlignment.CENTER,
            scroll=ft.ScrollMode.HIDDEN,
        )

        modal = ft.AlertDialog(
            modal=True,
            content=modal_content,
            actions_alignment=ft.MainAxisAlignment.CENTER,
            adaptive=True,
            title=ft.Text("Preview datasets summary"),
        )

        def save_file(e: ft.FilePickerResultEvent):
            if e.path:
                df.to_csv(e.path, index=False)

        def close_modal(e):
            modal.open = False
            self.page.update()

        def do_export(e):
            file_picker = ft.FilePicker(on_result=save_file)
            self.page.overlay.append(file_picker)
            self.page.update()
            file_picker.save_file(
                "Save csv table",
                # initial_directory=str(self._current_path),
                initial_directory=str(self._paths[0]),
            )
            self.page.update()

        actions = [
            ft.TextButton("close", on_click=close_modal),
            ft.TextButton("export", on_click=do_export),
        ]
        modal.actions = actions

        return modal

    def on_click_export(self, e: ft.ControlEvent):
        modal = self._get_export_window()

        self.page.dialog = modal
        modal.open = True
        self.page.update()

    def show_hidden_files(self, e):
        c: ft.Checkbox = e.control

        self._hidden_files = bool(c.value)
        # self._switch_dir(self._current_path)
        self._switch_dir(self._paths[0])
        e.page.update()

    def go_to_path(self, e):
        self.set_loading()
        c = e.control
        path = c.value
        if path == "~":
            path = str(Path(path).expanduser())

        if self.fs.exists(path) and self.fs.is_dir(path):
            self._switch_dir(path)
            e.page.update()

        elif not self.fs.exists(path):
            alert = ft.AlertDialog(title=ft.Text("No such file or directory"))
            e.page.dialog = alert
            alert.open = True
            e.page.update()

        elif not self.fs.is_dir(path):
            alert = ft.AlertDialog(title=ft.Text("Not a directory"))
            e.page.dialog = alert
            alert.open = True
            e.page.update()
        self.hide_loading()

    def select_path_from_dd(self, e):
        self.set_loading()

        c = e.control
        path = c.content.value
        self._switch_dir(path)

        dropdown = self.dropdown.current
        dropdown.content.controls.clear()
        dropdown.visible = False
        e.page.update()

        self.hide_loading()

    def close_dropdown_path(self, e):
        dropdown = self.dropdown.current
        dropdown.visible = False
        dropdown.content.controls.clear()

        self.search_bar.current.value = os.path.split(self._paths[0])[1]
        e.page.update()

    def show_dropdown_with_paths(self, e):
        c: ft.TextField = e.control

        target_stack = self.content.controls[-1]
        dropdown: ft.Container = target_stack
        dropdown.content.controls.clear()

        def on_hover(e):
            e.control.bgcolor = ft.colors.BLUE if e.data == "true" else None
            e.page.update()

        paths = []
        current_path = c.value
        size = 14

        cur_path = os.path.split(current_path)
        # for p in self.fs.list_files_in_dir(current_path):
        for p in self.fs.glob(cur_path[0], f"{cur_path[1]}*"):
            if not self.fs.is_dir(p):
                continue

            text = ft.Text(p, size=size, color=ft.colors.WHITE)
            paths.append(
                ft.Container(
                    text,
                    on_hover=on_hover,
                    padding=ft.padding.only(left=10, right=10, top=6, bottom=6),
                    on_click=self.select_path_from_dd,
                )
            )

        if paths:
            dropdown.visible = True
            dropdown.content.controls = paths

        c.focus()
        e.page.update()

    def _dicom_to_dict(self, file: pydicom.FileDataset):
        return {
            k.strip(): v.strip()
            for s in str(file).split("\n")
            for k, v in re.findall(r"(\(.*\) .*)([A-Z]{2}:.*)", s)
        }

    def open_dicom_properties(self, e):
        c = e.control
        fn = c.content.controls[-1].value

        # fn_path = Path(self._current_path).joinpath(fn)
        fn_path = Path(self._paths[0]).joinpath(fn)
        fn_path = c.key
        fileobj = self.fs.get_fileobj(fn_path)
        try:
            file = pydicom.dcmread(fileobj)
        finally:
            fileobj.close()

        data = self._dicom_to_dict(file)

        alert = ft.AlertDialog(
            adaptive=True,
            open=True,
            actions_alignment=ft.MainAxisAlignment.CENTER,
            title=ft.Row(
                controls=[
                    ft.Text(
                        f"Metadata for {fn}",
                        size=22,
                        weight=ft.FontWeight.BOLD,
                    )
                ],
                alignment=ft.MainAxisAlignment.CENTER,
            ),
            content=ft.Column(
                alignment=ft.MainAxisAlignment.START,
                controls=[
                    ft.Row(
                        controls=[
                            ft.Container(
                                ft.Text(
                                    "Key",
                                    size=19,
                                    weight=ft.FontWeight.W_500,
                                ),
                                alignment=ft.alignment.center_right,
                                expand=True,
                                padding=ft.padding.all(10),
                            ),
                            ft.Container(
                                ft.Text(
                                    "Value",
                                    size=19,
                                    weight=ft.FontWeight.W_500,
                                ),
                                alignment=ft.alignment.center_left,
                                expand=True,
                                padding=ft.padding.all(10),
                            ),
                        ],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    ),
                    ft.ListView(
                        width=400,
                        height=400,
                        controls=[
                            ft.Row(
                                controls=[
                                    ft.Container(
                                        expand=True,
                                        alignment=ft.alignment.center_right,
                                        content=ft.Text(
                                            re.split(r"\(.*\)", k)[1][:15],
                                            size=18,
                                            selectable=True,
                                            tooltip=k,
                                            weight=ft.FontWeight.W_400,
                                        ),
                                        padding=ft.padding.all(2),
                                    ),
                                    ft.Container(
                                        expand=True,
                                        alignment=ft.alignment.center_left,
                                        content=ft.Text(
                                            v,
                                            size=18,
                                            selectable=True,
                                            tooltip=v,
                                            weight=ft.FontWeight.W_400,
                                        ),
                                        padding=ft.padding.all(2),
                                    ),
                                ],
                                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                            )
                            for k, v in data.items()
                        ],
                    ),
                ],
            ),
        )

        def copy_to_clipboard(e):
            p: ft.Page = e.page
            p.set_clipboard(str(file))

        def close_modal(e):
            e.page.close_dialog()

        buttons = [
            ft.ElevatedButton("Copy to clicboard", on_click=copy_to_clipboard),
            ft.ElevatedButton("Close", on_click=close_modal),
        ]

        alert.actions = buttons
        e.page.dialog = alert
        e.page.update()

    def set_loading(self):
        ring = ft.ProgressRing()
        content = ft.Container(
            blur=10,
            expand=True,
            expand_loose=True,
            alignment=ft.alignment.center,
            content=ft.Row(
                controls=[
                    ft.Column(
                        expand=True,
                        alignment=ft.MainAxisAlignment.CENTER,
                        controls=[
                            ft.Row(
                                controls=[
                                    ring,
                                    ft.Text(value="Loading ...", size=19),
                                ],
                                alignment=ft.MainAxisAlignment.CENTER,
                            )
                        ],
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                    )
                ],
                expand=True,
                alignment=ft.MainAxisAlignment.CENTER,
            ),
        )
        self._previos_content = self.content
        self.content.controls.append(content)
        self.page.update()

    def hide_loading(self):
        del self.content.controls[-1]
        self.page.update()

    def on_focus_searchbar(self, e):
        c: ft.TextField = e.control
        # c.value = str(self._current_path)
        c.value = str(self._paths[0])
        e.page.update()

    def go_back(self, e):
        self.set_loading()

        e: ft.Container = e.control
        # path = self.fs.parrent_dir(self._current_path)
        path = self.fs.parrent_dir(self._paths[0])
        self._switch_dir(path)
        e.page.update()

        self.hide_loading()

    def on_dir_clicked(self, e):
        self.set_loading()

        c: ft.Container = e.control
        # path = Path(self._current_path).joinpath(
        #     str(c.content.controls[-1].value)
        # )
        path = c.key
        self._switch_dir(path)
        e.page.update()

        self.hide_loading()

    def _switch_dir(self, path: str):
        self._nesting_level = 1

        path = str(path)
        self._current_path = path
        self._paths = [path]
        files = self.get_files(path)

        self.files_container.current.content.controls = files

        sort_control = self._get_enabled_sort_filter()
        if sort_control:
            sorted_filter_name = sort_control.content.controls[0].value.lower()
            filter_icon = sort_control.content.controls[1]

            if filter_icon.name == ft.icons.KEYBOARD_ARROW_DOWN_SHARP:
                reverse = True
            else:
                reverse = False

            self.files_container.current.content.controls = self._sort_files_by(
                filter_name=sorted_filter_name, reverse=reverse
            )

        c = self.content.controls[0]
        c.controls[0].content.controls[1].value = os.path.split(path)[1]

    def expand_folder(self, e):
        self._nesting_level += 1

        icon_control = cast(ft.Icon, e.control.content)
        dirpath = icon_control.key
        self._paths.append(dirpath)

        row: ft.Container = e.control.content.row
        subfiles = self.get_files(dirpath)

        files_rows = self.files_container.current.content.controls.copy()
        for i, file_row in enumerate(files_rows):
            file_row = cast(ft.Container, file_row)
            if file_row == row:
                files_rows[i + 1 : i + 1] = subfiles

        self.files_container.current.content.controls = files_rows

        e.control.on_click = self.collapse_folder
        icon_control.name = ft.icons.KEYBOARD_ARROW_DOWN

        self.page.update()

    def collapse_folder(self, e):
        self._nesting_level -= 1

        icon_control = cast(ft.Icon, e.control.content)
        dirpath = icon_control.key
        del self._paths[-1]
        files = self.files_container.current.content.controls.copy()

        new_files = []
        for file_control in files:
            key = file_control.key
            if not (key.startswith(dirpath) and key != dirpath):
                new_files.append(file_control)

        self.files_container.current.content.controls = new_files

        e.control.on_click = self.expand_folder
        icon_control.name = ft.icons.KEYBOARD_ARROW_RIGHT

        self.page.update()

    def get_files(self, path: str = ".") -> list[ft.Container]:
        rows_files = []
        mock_icon = ft.VerticalDivider(22)

        def append_rows(path):
            nonlocal rows_files
            path = str(path)

            if self._hidden_files is False and Path(path).name.startswith("."):
                return

            icon = [
                mock_icon,
                ft.Icon(
                    ft.icons.INSERT_DRIVE_FILE_OUTLINED, size=self.icon_size
                ),
            ]
            on_click = None

            if self.fs.is_dir(path):
                icon = [
                    ft.Container(
                        ft.Icon(
                            ft.icons.KEYBOARD_ARROW_RIGHT, size=28, key=path
                        ),
                        on_click=self.expand_folder,
                    ),
                    ft.Icon(ft.icons.FOLDER_ROUNDED, size=self.icon_size),
                ]
                on_click = self.on_dir_clicked

            elif self.fs.is_has_ext(path, ext=DICOM_EXT):
                on_click = self.open_dicom_properties

            size = self._get_size_of_file(str(path))

            if not isinstance(size, tuple):
                raw_size = size
                size = hr_size(size)
            else:
                raw_size = size[0]

            size = f"{size[0]} {size[1]}"

            row = ft.Container(
                ft.Row(
                    controls=[
                        ft.Container(
                            ft.Row(icon + [ft.Text(os.path.split(path)[-1])]),
                            on_click=on_click,
                            key=path,
                        ),
                        ft.Text(size),
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    expand=True,
                ),
                padding=ft.padding.only(
                    right=self.padding_left_right,
                    left=self.padding_left_right * self._nesting_level,
                ),
            )
            if on_click == self.on_dir_clicked:
                icon[0].content.row = row

            row.key = path
            row.raw_size = raw_size
            rows_files.append(row)

        with ThreadPoolExecutor(os.cpu_count() * 2) as e:
            start = time.time()
            e.map(append_rows, self.fs.list_files_in_dir(path))
        print(f"append rows took {time.time() - start}")

        rows_files = sorted(
            rows_files,
            key=lambda c: c.content.controls[0].content.controls[-1].value,
        )
        return rows_files

    def _get_size_of_file(self, path: str):
        if self.fs.is_dir(path):
            return self.fs.get_size_of_dir(path), "objects"

        return self.fs.raw_size_of_file(path)


class LeftMenu(ft.Container):
    def __init__(self, explorer: CustomExplorer) -> None:
        super().__init__(
            padding=ft.padding.only(left=10, right=10, top=20, bottom=20),
        )
        icon = ft.icons.SETTINGS
        self.theme_btn = ft.TextButton(
            "System mode",
            icon=icon,
            on_click=self.update_theme,
        )

        self.switcher = StorageSwitcher(explorer)
        self.mode_btn = ft.TextButton(
            "Switch dataset storage",
            icon=ft.icons.DATASET,
            on_click=self.switcher.open_window,
        )
        self.content = ft.Column(
            controls=[
                ft.Column(
                    controls=[self.theme_btn, self.mode_btn],
                ),
                # ft.Column(
                #     controls=[ft.Text("arg1"), ft.Text("arg2")],
                #     alignment=ft.MainAxisAlignment.END,
                #     expand=True,
                # ),
            ],
        )
        self._explorer = explorer

    def update_properties_folder(self):
        fs = self._explorer.fs
        path = self._explorer._current_path
        files = fs.list_files_recursively(path)

        total_size = sum(fs.size_in_bytes(f) for f in files)
        hr_size = get_human_readable_size(total_size)
        count = len(files)

        control = ft.Column(
            controls=[
                ft.DataTable(
                    columns=[
                        ft.DataColumn(ft.Text("Size")),
                        ft.DataColumn(ft.Text("DCM Files count")),
                    ],
                    rows=[
                        ft.DataRow(
                            cells=[
                                ft.DataCell(ft.Text(hr_size)),
                                ft.DataCell(ft.Text(count)),
                            ]
                        )
                    ],
                )
            ],
            expand=True,
            alignment=ft.MainAxisAlignment.END,
        )
        self.content.controls[-1] = control
        self.page.update()

    def update_theme(self, e: ft.ControlEvent):
        if self.page.theme_mode.value == "dark":
            self.page.theme_mode = ft.ThemeMode.SYSTEM
            selected_icon = ft.icons.SETTINGS
            info = "System mode"

        elif self.page.theme_mode.value == "light":
            self.page.theme_mode = ft.ThemeMode.DARK
            selected_icon = ft.icons.DARK_MODE
            info = "Dark mode"

        elif self.page.theme_mode.value == "system":
            self.page.theme_mode = ft.ThemeMode.LIGHT
            selected_icon = ft.icons.LIGHT_MODE
            info = "Light mode"

        self.theme_btn.icon = selected_icon
        self.theme_btn.text = info
        self.page.update()


class CorouselStorages(ft.Container):
    def __init__(self, storage_names: list[str], explorer: CustomExplorer):
        super().__init__(padding=ft.padding.all(5))
        self._modes = storage_names
        self._explorer_instance = explorer

        self.type_storages_controls: list[ft.Row, ft.Row] = None
        self._rows_width = 500
        self._rows_height = 100

        self._build_corousel()

    def _on_hover(self, e):
        border_color = get_border_color(e.page)
        container_color = (
            ft.colors.BLACK
            if border_color == ft.colors.WHITE
            else ft.colors.WHITE
        )

        e.control.bgcolor = (
            ft.colors.PRIMARY if e.data == "true" else container_color
        )
        e.page.update()

    def _get_modes_as_controls(
        self, modes: list[str] = None
    ) -> list[ft.Container]:
        modes = modes or self._modes
        page = self._explorer_instance.page

        border_color = get_border_color(page)
        text_color = border_color
        container_color = (
            ft.colors.BLACK
            if border_color == ft.colors.WHITE
            else ft.colors.WHITE
        )

        return [
            ft.Container(
                content=ft.Text(
                    mode,
                    size=20,
                    text_align=ft.TextAlign.CENTER,
                    style=ft.TextStyle(weight=ft.FontWeight.W_600),
                    color=text_color,
                ),
                border=ft.border.all(2, border_color),
                border_radius=ft.border_radius.all(6),
                alignment=ft.alignment.center,
                opacity=1,
                height=self._rows_height,
                width=150,
                padding=ft.padding.all(10),
                on_click=self._on_click_storage,
                bgcolor=container_color,
                on_hover=self._on_hover,
            )
            for mode in modes
        ]

    def _on_click_storage(self, e):
        storage_type: str = e.control.content.value

        if storage_type.lower().count("local"):
            fs = LocalFileSystem()
        elif storage_type.lower().count("s3"):
            fs = S3FileSystem()

        self._explorer_instance.set_loading()

        self._explorer_instance._filesystem = fs
        self._explorer_instance._switch_dir(fs.default_path)
        self._explorer_instance.page.update()

        self._explorer_instance.hide_loading()

    def _build_corousel(self):
        rows_width = self._rows_width
        rows_height = self._rows_height
        contents = self._get_modes_as_controls()

        storages_row = ft.Row(
            controls=contents,
            width=rows_width,
            height=rows_height,
            alignment=ft.MainAxisAlignment.CENTER,
            expand=True,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=20,
        )

        self.type_storages_controls = [storages_row]
        carousel = ft.Container(
            ft.Row(
                controls=[
                    ft.Stack(
                        controls=self.type_storages_controls,
                        alignment=ft.alignment.center,
                        height=rows_height,
                        width=rows_width,
                    ),
                ],
                alignment=ft.MainAxisAlignment.CENTER,
            )
        )

        self.content = carousel

    def _switch_storage_type(self, contents: list[ft.Control]):
        rows = self.type_storages_controls
        overheads_containers = contents[:-1]
        overheads_containers[0].height = 95
        overheads_containers[1].height = 120
        overheads_containers[2].height = 95

        overheads = rows[0]
        overheads.controls = overheads_containers

        for c in overheads_containers:
            c.opacity = 1
            c.width = 120

        last_item_container = contents[-1]
        last_item_container.opacity = 0.5
        last_item_container.width = 140
        last_item_container.height = self._rows_height
        last_item = rows[1]
        last_item.controls = [last_item_container]
        last_item.visible = False

        self.type_storages_controls = [overheads, last_item]

    def on_click_previous_storage_type(self, e):
        stack: ft.Stack = self.content.content.controls[1]

        rows = self.type_storages_controls
        modes = [c for r in rows for c in r.controls]
        contents = modes[1:] + modes[:1]

        self._switch_storage_type(contents)
        stack.controls = self.type_storages_controls
        self.page.update()

    def on_click_next_storage_type(self, e):
        stack: ft.Stack = self.content.content.controls[1]
        rows = self.type_storages_controls.copy()
        modes = [c for r in rows for c in r.controls]
        contents = modes[-1:] + modes[:-1]

        self._switch_storage_type(contents)
        stack.controls = self.type_storages_controls
        self.page.update()


class StorageSwitcher(ft.Control):
    def __init__(self, explorer: CustomExplorer):
        super().__init__()
        self._modes = [
            "Local Folder",
            "S3 Bucket Storage",
        ]
        self._explorer_instance = explorer

    def _get_modal(self, page: ft.Page):
        model_content = CorouselStorages(self._modes, self._explorer_instance)

        modal = ft.Container(
            expand=True,
            content=model_content,
            alignment=ft.alignment.center,
            blur=10,
            on_click=self._close_modal,
            animate_opacity=ft.animation.Animation(500, ft.AnimationCurve.EASE),
        )
        return modal

    async def _close_modal(self, e):
        page: ft.Page = e.page
        modal: ft.Container = e.control

        modal.opacity = 0 if modal.opacity == 1 else 1
        page.update()
        del page.overlay[-1]

        await asyncio.sleep(0.5)
        page.update()

    async def open_window(self, e):
        page = cast(ft.Page, e.page)

        modal = self._get_modal(page=page)

        modal.opacity = 0
        page.overlay.append(modal)
        page.update()

        await asyncio.sleep(0.01)
        modal.opacity = 1
        page.update()


class ColScheme(ft.ColorScheme):
    def __init__(self):
        super().__init__()
        self.primary = ft.colors.AMBER_700
        self.on_primary = ft.colors.BLACK

        self.primary_container = ft.colors.RED
        self.on_primary_container = ft.colors.GREEN_ACCENT_200

        self.secondary = ft.colors.GREEN_ACCENT_200
        self.on_secondary_container = ft.colors.GREEN_ACCENT_400

        self.tertiary = ft.colors.LIGHT_BLUE_ACCENT_100
        self.on_tertiary = ft.colors.LIGHT_BLUE_ACCENT_400
        self.on_tertiary_container = ft.colors.CYAN_ACCENT_400


class LightTheme(ft.Theme):
    def __init__(self):
        super().__init__()
        self.color_scheme = ColScheme()
        self.font_family = "custom"

        self.checkbox_theme = ft.CheckboxTheme()
        self.checkbox_theme.border_side = ft.BorderSide(2, "black")


class ColSchemeDark(ft.ColorScheme):
    def __init__(self):
        super().__init__()
        self.primary = ft.colors.BLUE_ACCENT_400
        self.on_primary = ft.colors.WHITE

        self.secondary = ft.colors.GREEN_ACCENT_200
        self.on_secondary_container = ft.colors.GREEN_ACCENT_400

        self.tertiary = ft.colors.LIGHT_BLUE_ACCENT_100
        self.on_tertiary = ft.colors.LIGHT_BLUE_ACCENT_400
        self.on_tertiary_container = ft.colors.CYAN_ACCENT_400


class DarkTheme(LightTheme):
    def __init__(self):
        super().__init__()
        self.color_scheme = ColSchemeDark()


def set_page_style(page: ft.Page):
    page.padding = 10

    page.fonts = {
        "custom": "montserrat.bold.ttf",
    }

    page.theme_mode = ft.ThemeMode.SYSTEM

    page.theme = LightTheme()
    page.dark_theme = DarkTheme()
    page.vertical_alignment = ft.MainAxisAlignment.START

    return page


async def main(page: ft.Page):
    """Entrypoint."""
    page.title = "Dataset Walker"
    page = set_page_style(page)
    explorer = CustomExplorer(page)
    left_menu = LeftMenu(explorer)

    def on_keyboard(e: ft.KeyboardEvent):
        if e.key.lower() == "e":
            if explorer.export.offset.x == 0:
                explorer.export.offset = ft.Offset(5, 0)
            else:
                explorer.export.offset = ft.Offset(0, 0)
        elif e.key in ["Arrow Down", "Arrow Up"]:
            dropdown = explorer.dropdown.current

            if not dropdown.visible:
                return

            for i, control in enumerate(dropdown.content.controls):
                control: ft.Container

                if not control.bgcolor:
                    continue

                needed_index = max(i - 1, 0) if e.key == "Arrow Up" else i + 1
                if needed_index > (len(dropdown.content.controls) - 1):
                    needed_index = len(dropdown.content.controls) - 1

                dropdown.content.controls[
                    needed_index
                ].bg_color = ft.colors.BLUE
                control.bgcolor = None

                break

            explorer.dropdown.current = dropdown

        page.update()

    page.on_keyboard_event = on_keyboard
    page_content = ft.Row(
        controls=[left_menu, explorer],
        expand=True,
        vertical_alignment=ft.CrossAxisAlignment.START,
    )

    page.add(page_content)
    # left_menu.update_properties_folder()


def test():
    c = CustomExplorer()
    c._get_dataset("dataset/ishemia_21022022")


if __name__ == "__main__":
    ft.app(target=main)
    # test( )
