from dataclasses import dataclass
from typing import Any, Dict, Union
from pathlib import Path, PurePath
import json

import pandas as pd

@dataclass
class FileSystemClient:
    root_path: Path = Path(__file__).parent.parent.parent.absolute() / "data_bucket"
    sub_path: PurePath = None

    def get_folder(self) -> PurePath:
        if self.root_path and self.sub_path:
            return self.root_path / self.sub_path
        elif self.root_path and not self.sub_path:
            return self.root_path
        elif self.sub_path:
            return self.sub_path
        else:
            return PurePath()

    def load_str(self, source: Union[str, PurePath]) -> str:
        filepath = self.get_folder() / source
        with open(filepath) as f:
            return f.read()

    def load_json(self, source: Union[str, PurePath]) -> Dict[str, Any]:
        return json.loads(self.load_str(source))

    def load_csv(self, source: Union[str, PurePath]) -> pd.DataFrame:
        filepath = self.get_folder() / source
        return pd.read_csv(filepath)

    def save_str(self, blob: str, filename: str) -> Path:
        filepath = self.get_folder() / filename
        FileSystemClient.mkdir(filepath)
        with open(filepath, "w") as f:
            f.write(blob)
        return filepath

    def save_json(self, json_dict: Dict[str, Any], filename: str) -> Path:
        filename = f'{filename}.json'
        json_str = json.dumps(json_dict, default=str)
        return self.save_str(json_str, filename)

    def save_csv(self, df: pd.DataFrame, filename: str) -> Path:
        filepath = self.get_folder() / f'{filename}.csv'
        FileSystemClient.mkdir(filepath)
        df.to_csv(filepath, index=False)
        return filepath

    @staticmethod
    def mkdir(filepath: Path):
        '''
        Creates a directory if its missing. Safe to call multiple times. Accepts full filepaths or directory paths.
        '''
        if not filepath.is_dir():
            filepath = filepath.parent
        Path(filepath).mkdir(exist_ok=True, parents=True)

if __name__ == "__main__":
    client_a = FileSystemClient()
    a_json = client_a.load_json('sample/data.json')
    print(a_json['version'])

    client_b = FileSystemClient(sub_path=PurePath('sample'))
    b_json = client_b.load_json('data.json')
    print(b_json['version'])

    client_a.save_json(a_json, 'test_a')

    client_b.save_json(b_json, 'test_b')

    client_c = FileSystemClient(sub_path=PurePath('tests/test_dir'))
    client_c.save_json(b_json, 'test_c')
