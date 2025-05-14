from django import forms

class CSVFileForm(forms.Form):
    csv_file = forms.FileField(label="Choose a CSV file", required=True, widget=forms.ClearableFileInput(attrs={'accept': '.csv'}))
