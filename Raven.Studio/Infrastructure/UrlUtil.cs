using System;
using System.Windows;
using System.Windows.Browser;
using System.Windows.Input;

namespace Raven.Studio.Infrastructure
{
	public static class UrlUtil
	{
		static UrlUtil()
		{
			Url = Application.Current.Host.NavigationState;
			Application.Current.Host.NavigationStateChanged += (sender, args) => Url = args.NewNavigationState;
		}

		public static string Url { get; private set; }

		private static void Navigate(Uri source)
		{
			Execute.OnTheUI(() => Application.Current.Host.NavigationState = source.ToString());
		}

		public static void Navigate(string url, bool dontOpenNewTag = false)
		{
			if (url == null)
				return;

			url = new UrlParser(url).BuildUrl();

			Execute.OnTheUI(() =>
			                	{
									if (Keyboard.Modifiers == ModifierKeys.Control && dontOpenNewTag == false)
			                		{
			                			OpenUrlOnANewTab(url);
										return;
			                		}

			                		Navigate((new Uri(url, UriKind.Relative)));
			                	});
		}

		private static void OpenUrlOnANewTab(string url)
		{
			var hostUrl = HtmlPage.Document.DocumentUri.OriginalString;
			var fregmentIndex = hostUrl.IndexOf('#');
			string host = fregmentIndex != -1 ? hostUrl.Substring(0, fregmentIndex) : hostUrl;

			HtmlPage.Window.Navigate(new Uri(host + "#" + url, UriKind.Absolute), "_blank");
		}
	}
}